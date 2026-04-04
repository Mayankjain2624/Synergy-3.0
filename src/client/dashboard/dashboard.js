import { DbConnection} from '/module_bindings/index.js';

const SPACETIMEDB_URI = window.GRIDFORGOOD_URI ?? 'wss://maincloud.spacetimedb.com';
const DB_NAME = window.GRIDFORGOOD_DB_NAME ?? 'hack';
const ACTIVE_WINDOW_MICROS = 2_000_000n;

const canvas = document.getElementById('mandelbrot');
const context = canvas.getContext('2d');

const activeNodesEl = document.getElementById('active-nodes');
const chunksProcessedEl = document.getElementById('chunks-processed');
const completionEl = document.getElementById('completion');

const drawnChunks = new Set();

function clearCanvas() {
	context.fillStyle = '#02040a';
	context.fillRect(0, 0, canvas.width, canvas.height);
	drawnChunks.clear();
}

function clearChunk(chunk) {
	context.fillStyle = '#02040a';
	context.fillRect(
		Number(chunk.tileX) * Number(chunk.width),
		Number(chunk.tileY) * Number(chunk.height),
		Number(chunk.width),
		Number(chunk.height)
	);
	drawnChunks.delete(chunk.chunkId.toString());
}

function unwrapOption(value) {
	if (value === undefined || value === null) {
		return undefined;
	}

	if (typeof value === 'object' && 'tag' in value) {
		if (value.tag === 'some' || value.tag === 'Some') {
			return value.value;
		}
		return undefined;
	}

	if (typeof value === 'object' && 'some' in value) {
		return value.some;
	}

	return value;
}

function parsePixelData(rawData) {
	if (!rawData) {
		return null;
	}
	try {
		const parsed = JSON.parse(rawData);
		if (!Array.isArray(parsed)) {
			return null;
		}
		return new Uint8ClampedArray(parsed);
	} catch {
		return null;
	}
}

function paintChunk(chunk) {
	const pixelData = unwrapOption(chunk.pixelData);
	if (drawnChunks.has(chunk.chunkId.toString()) || !pixelData) {
		return;
	}

	const pixels = parsePixelData(pixelData);
	if (!pixels) {
		return;
	}

	const imageData = new ImageData(pixels, Number(chunk.width), Number(chunk.height));
	context.putImageData(
		imageData,
		Number(chunk.tileX) * Number(chunk.width),
		Number(chunk.tileY) * Number(chunk.height)
	);

	drawnChunks.add(chunk.chunkId.toString());
}

function updateMetrics(conn) {
	let total = 0;
	let completed = 0;

	for (const chunk of conn.db.chunkQueue.iter()) {
		total += 1;
		if (chunk.status === 'completed') {
			completed += 1;
			paintChunk(chunk);
		}
	}

	const nowMicros = BigInt(Date.now()) * 1000n;
	let activeNodes = 0;
	for (const node of conn.db.nodeStatus.iter()) {
		if (nowMicros - node.lastSeenMicros <= ACTIVE_WINDOW_MICROS) {
			activeNodes += 1;
		}
	}

	const completion = total > 0 ? Math.floor((completed / total) * 100) : 0;
	activeNodesEl.textContent = `${activeNodes}`;
	chunksProcessedEl.textContent = `${completed}`;
	completionEl.textContent = `${completion}%`;
}

DbConnection.builder()
	.withUri(SPACETIMEDB_URI)
	.withDatabaseName(DB_NAME)
	.onConnect(conn => {
		clearCanvas();

		conn.db.chunkQueue.onInsert((_ctx, row) => {
			if (row.status === 'completed') {
				paintChunk(row);
			} else {
				clearChunk(row);
			}
			updateMetrics(conn);
		});

		conn.db.chunkQueue.onUpdate((_ctx, oldRow, row) => {
			if (row.status === 'completed') {
				paintChunk(row);
			} else if (oldRow.status === 'completed' || drawnChunks.has(row.chunkId.toString())) {
				clearChunk(row);
			}
			updateMetrics(conn);
		});

		conn.db.gridConfig.onInsert(() => {
			clearCanvas();
			updateMetrics(conn);
		});

		conn.db.gridConfig.onUpdate(() => {
			clearCanvas();
			updateMetrics(conn);
		});

		conn.db.nodeStatus.onInsert(() => updateMetrics(conn));
		conn.db.nodeStatus.onUpdate(() => updateMetrics(conn));

		const subscription = conn.subscriptionBuilder();
		if (typeof subscription.subscribeToAllTables === 'function') {
			subscription.subscribeToAllTables();
		} else if (typeof subscription.subscribeToAll === 'function') {
			subscription.subscribeToAll();
		} else {
			subscription.subscribe(['SELECT * FROM chunk_queue', 'SELECT * FROM node_status']);
		}
		updateMetrics(conn);
		window.setInterval(() => updateMetrics(conn), 1000);
	})
	.onConnectError((_ctx, err) => {
		console.error('Dashboard connection error:', err);
	})
	.build();
