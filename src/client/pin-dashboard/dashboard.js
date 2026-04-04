import { DbConnection } from '/module_bindings/index.js';

const SPACETIMEDB_URI = window.GRIDFORGOOD_URI ?? 'wss://maincloud.spacetimedb.com';
const DB_NAME = window.GRIDFORGOOD_DB_NAME ?? 'hack';
const ACTIVE_WINDOW_MICROS = 2_000_000n;
const DEFAULT_PIN_TARGET_HASH = '919c68ff757c3fe518643fbe8424b381ba9e1aaf1eac547a2b7c759a4f687793';

const activeNodesEl = document.getElementById('active-nodes');
const rangesCheckedEl = document.getElementById('ranges-checked');
const completionEl = document.getElementById('completion');
const targetHashEl = document.getElementById('target-hash');
const rangeLogEl = document.getElementById('range-log');
const foundBannerEl = document.getElementById('found-banner');

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

function updateDashboard(conn) {
	let total = 0;
	let completed = 0;
	const recent = [];

	for (const row of conn.db.pinChunkQueue.iter()) {
		total += 1;
		if (row.status === 'completed') {
			completed += 1;
			const owner = unwrapOption(row.assignedNode);
			const ownerLabel = owner ? `${owner.toHexString().slice(0, 12)}...` : 'unknown';
			const foundPin = unwrapOption(row.foundPin);
			recent.push({
				chunkId: row.chunkId,
				text: `Range ${row.rangeStart}-${row.rangeEnd} by ${ownerLabel}${foundPin ? ` FOUND ${foundPin}` : ''}`,
			});
		}
	}

	const completion = total > 0 ? Math.floor((completed / total) * 100) : 0;
	rangesCheckedEl.textContent = `${completed}`;
	completionEl.textContent = `${completion}%`;

	const nowMicros = BigInt(Date.now()) * 1000n;
	let activeNodes = 0;
	for (const node of conn.db.nodeStatus.iter()) {
		if (nowMicros - node.lastSeenMicros <= ACTIVE_WINDOW_MICROS) {
			activeNodes += 1;
		}
	}
	activeNodesEl.textContent = `${activeNodes}`;

	const config = conn.db.pinCrackConfig.id.find(1);
	targetHashEl.textContent = config?.targetHash ?? DEFAULT_PIN_TARGET_HASH;

	rangeLogEl.innerHTML = '';
	recent
		.sort((a, b) => Number(b.chunkId - a.chunkId))
		.slice(0, 12)
		.forEach(item => {
			const li = document.createElement('li');
			li.textContent = item.text;
			rangeLogEl.appendChild(li);
		});

	const pinFound = unwrapOption(config?.pinFound);
	const foundBy = unwrapOption(config?.foundByNode);
	if (pinFound && foundBy && config?.foundAtMicros) {
		const elapsedMs = Number(config.foundAtMicros - config.startedAtMicros) / 1000;
		const elapsedSec = (elapsedMs / 1000).toFixed(2);
		foundBannerEl.style.display = 'block';
		foundBannerEl.textContent = `FOUND: PIN is ${pinFound} - found by ${foundBy.toHexString().slice(0, 12)}... in ${elapsedSec}s.`;
	} else if (!config || total === 0) {
		foundBannerEl.style.display = 'block';
		foundBannerEl.textContent = 'Waiting for pin task initialization. Click Reset PIN Task on a compute node once.';
	} else {
		foundBannerEl.style.display = 'none';
	}
}

DbConnection.builder()
	.withUri(SPACETIMEDB_URI)
	.withDatabaseName(DB_NAME)
	.onConnect(conn => {
		conn.db.pinChunkQueue.onInsert(() => updateDashboard(conn));
		conn.db.pinChunkQueue.onUpdate(() => updateDashboard(conn));
		conn.db.pinCrackConfig.onInsert(() => updateDashboard(conn));
		conn.db.pinCrackConfig.onUpdate(() => updateDashboard(conn));
		conn.db.nodeStatus.onInsert(() => updateDashboard(conn));
		conn.db.nodeStatus.onUpdate(() => updateDashboard(conn));

		const subscription = conn.subscriptionBuilder();
		if (typeof subscription.subscribeToAllTables === 'function') {
			subscription.subscribeToAllTables();
		} else if (typeof subscription.subscribeToAll === 'function') {
			subscription.subscribeToAll();
		} else {
			subscription.subscribe([
				'SELECT * FROM pin_chunk_queue',
				'SELECT * FROM pin_crack_config',
				'SELECT * FROM node_status',
			]);
		}

		updateDashboard(conn);
		window.setInterval(() => updateDashboard(conn), 1000);
	})
	.onConnectError((_ctx, err) => {
		console.error('PIN dashboard connection error:', err);
	})
	.build();
