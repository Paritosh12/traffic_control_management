let draggedType = null;
let sensorCounter = 0;
let activeSensors = []; 
let droppedNodes = [];
let updateDataInterval = null; 
let windowSettingsCache = [];
let graphRefreshInterval = null;
let activeGraphConfig = null;
let queryMetadata = {
    sources: [
        { name: 'sensor_stream', fields: ['pkt_id', 'intersection_id', 'road_id', 'signal_id', 'vehicle_count', 'avg_speed', 'occupancy', 'ts'] },
        { name: 'event_stream', fields: ['event_id', 'event_type', 'intersection_id', 'road_id', 'priority', 'ts'] },
        { name: 'command_stream', fields: ['cmd_id', 'signal_id', 'action', 'duration', 'reason', 'ts'] }
    ],
    operations: ['none', 'sum', 'count', 'avg', 'min', 'max']
};


function dragStart(ev, type) { draggedType = type; }
function allowDrop(ev) { ev.preventDefault(); }

function drop(ev) {
    ev.preventDefault();
    if(!draggedType) return;
    const workspace = document.getElementById('workspace');
    const rect = workspace.getBoundingClientRect();
    let x = ev.clientX - rect.left;
    let y = ev.clientY - rect.top;

    let title = draggedType;
    let icon = '';

    sensorCounter++;
    const nodeId = 'sensor-node-' + sensorCounter;
    
    const element = document.createElement('div');
    element.className = 'dropped-sensor';
    element.id = nodeId;
    element.style.left = x + 'px';
    element.style.top = y + 'px';
    
    element.innerHTML = `
        <span style="font-weight:bold; margin-right:5px;">${icon}</span> ${title}
        <button class="close-btn" onclick="removeNode('${nodeId}', '${draggedType}')">✕</button>
    `;
    
    element.onmousedown = function(e) {
        if(e.target.tagName === 'BUTTON') return;
        e.preventDefault();
        let shiftX = e.clientX - element.getBoundingClientRect().left;
        let shiftY = e.clientY - element.getBoundingClientRect().top;
        function moveAt(pageX, pageY) {
            element.style.left = pageX - shiftX - rect.left + 'px';
            element.style.top = pageY - shiftY - rect.top + 'px';
            drawConnections();
        }
        function onMouseMove(ev) { moveAt(ev.pageX, ev.pageY); }
        document.addEventListener('mousemove', onMouseMove);
        element.onmouseup = function() {
            document.removeEventListener('mousemove', onMouseMove);
            element.onmouseup = null;
        };
    };
    element.ondragstart = function() { return false; };
    
    workspace.appendChild(element);
    droppedNodes.push({ id: nodeId, el: element, type: draggedType });
    
    if(!activeSensors.includes(draggedType)) {
        activeSensors.push(draggedType);
        updateActiveSensorsConf();
    }
    drawConnections();
    draggedType = null;
}

function removeNode(nodeId, type) {
    const el = document.getElementById(nodeId);
    if(el) {
        el.remove();
        droppedNodes = droppedNodes.filter(n => n.id !== nodeId);
        const hasRemaining = droppedNodes.some(n => n.type === type);
        if(!hasRemaining) {
            activeSensors = activeSensors.filter(s => s !== type);
            updateActiveSensorsConf();
        }
        drawConnections();
    }
}

//connections
function drawConnections() {
    const svg = document.getElementById('svg-layer');
    if (!svg) return;
    svg.innerHTML = '';
    const controls = document.getElementById('controls-panel');
    const cRect = getRelativeRect(controls);
    const cCenter = { x: cRect.left + cRect.width/2, y: cRect.top + cRect.height/2 };
    
    droppedNodes.forEach(node => {
        const nRect = getRelativeRect(node.el);
        const nCenter = { x: nRect.left + nRect.width/2, y: nRect.top + nRect.height/2 };
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        const d = `M ${nCenter.x},${nCenter.y} Q ${nCenter.x},${(nCenter.y+cCenter.y)/2} ${cCenter.x},${cCenter.y}`;
        path.setAttribute('d', d);
        path.setAttribute('class', 'connection-line');
        svg.appendChild(path);
    });
}

function getRelativeRect(el) {
    const workspace = document.getElementById('workspace');
    const wRect = workspace.getBoundingClientRect();
    const elRect = el.getBoundingClientRect();
    return { left: elRect.left - wRect.left, top: elRect.top - wRect.top, width: elRect.width, height: elRect.height };
}


function showTable(viewId, btn) {
    document.querySelectorAll('.table-view').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.table-icon-btn').forEach(el => el.classList.remove('active'));
    document.getElementById('view-' + viewId).classList.add('active');
    btn.classList.add('active');
}

function toggleModal(show) {
    document.getElementById('settings-modal').classList.toggle('active', show);
    if (show) {
        loadWindowSettings();
    }
}

async function loadWindowSettings() {
    const container = document.getElementById('window-settings-list');
    if (!container) return;

    container.innerHTML = '<div class="settings-loading">Loading window settings...</div>';

    try {
        const response = await fetch('/api/settings/windows');
        const data = await response.json();

        if (!response.ok) {
            container.innerHTML = `<div class="settings-loading">${escapeHtml(data.error || 'Failed to load window settings')}</div>`;
            return;
        }

        windowSettingsCache = data.streams || [];
        renderWindowSettings(windowSettingsCache);
    } catch (err) {
        console.error('[loadWindowSettings] Error:', err);
        container.innerHTML = '<div class="settings-loading">Error loading window settings</div>';
    }
}

function renderWindowSettings(streams) {
    const container = document.getElementById('window-settings-list');
    if (!container) return;

    if (!streams.length) {
        container.innerHTML = '<div class="settings-loading">No stream windows found in traffic.xml</div>';
        return;
    }

    container.innerHTML = streams.map(stream => {
        const safeName = escapeHtml(stream.name);
        const type = stream.windowType || 'Sliding';
        const unit = stream.unit || '';
        const size = stream.size || '';

        return `
            <div class="window-setting-card" data-stream-name="${safeName}">
                <div class="window-setting-title">${safeName}</div>
                <div class="window-setting-grid">
                    <div class="form-group">
                        <label>Window Type</label>
                        <select data-field="windowType">
                            <option value="Sliding" ${type === 'Sliding' ? 'selected' : ''}>Sliding</option>
                            <option value="Tumbling" ${type === 'Tumbling' ? 'selected' : ''}>Tumbling</option>
                            <option value="Landmark" ${type === 'Landmark' ? 'selected' : ''}>Landmark</option>
                        </select>
                    </div>
                    <div class="form-group">
                        <label>Size</label>
                        <input type="number" min="1" data-field="size" value="${escapeHtml(size)}" placeholder="None">
                    </div>
                    <div class="form-group">
                        <label>Unit</label>
                        <select data-field="unit">
                            <option value="" ${unit === '' ? 'selected' : ''}>None</option>
                            <option value="seconds" ${unit === 'seconds' ? 'selected' : ''}>seconds</option>
                            <option value="packets" ${unit === 'packets' ? 'selected' : ''}>packets</option>
                            <option value="events" ${unit === 'events' ? 'selected' : ''}>events</option>
                        </select>
                    </div>
                </div>
            </div>
        `;
    }).join('');
}

async function saveWindowSettings() {
    const cards = document.querySelectorAll('.window-setting-card');
    const streams = Array.from(cards).map(card => ({
        name: card.dataset.streamName,
        windowType: card.querySelector('[data-field="windowType"]').value,
        size: card.querySelector('[data-field="size"]').value,
        unit: card.querySelector('[data-field="unit"]').value
    }));

    try {
        const response = await fetch('/api/settings/windows', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ streams })
        });
        const data = await response.json();

        if (!response.ok) {
            alert('Failed to save settings: ' + (data.message || data.error || 'Unknown error'));
            return;
        }

        windowSettingsCache = data.streams || [];
        renderWindowSettings(windowSettingsCache);
        loadSensors();
        toggleModal(false);
        alert('Window settings saved. Restart the system for running monitors to use the updated windows.');
    } catch (err) {
        console.error('[saveWindowSettings] Error:', err);
        alert('Failed to save settings: ' + err.message);
    }
}

function makePanelDraggable(panel) {
    let handle = panel.id === 'tables-panel' ? panel : (panel.querySelector('.panel-title') || panel);
    let pos1 = 0, pos2 = 0, pos3 = 0, pos4 = 0;
    
    handle.onmousedown = function(e) {
        const interactiveTarget = e.target.tagName === 'BUTTON'
            || e.target.tagName === 'INPUT'
            || e.target.tagName === 'SELECT'
            || e.target.tagName === 'TEXTAREA'
            || e.target.closest('.sensor-card')
            || e.target.closest('.table-view-container')
            || e.target.closest('.table-icon-btn');

        if (interactiveTarget) return;
        e = e || window.event;
        e.preventDefault();
        pos3 = e.clientX;
        pos4 = e.clientY;
        document.onmouseup = function() {
            document.onmouseup = null;
            document.onmousemove = null;
        };
        document.onmousemove = function(e) {
            e = e || window.event;
            e.preventDefault();
            pos1 = pos3 - e.clientX;
            pos2 = pos4 - e.clientY;
            pos3 = e.clientX;
            pos4 = e.clientY;
            panel.style.top = (panel.offsetTop - pos2) + "px";
            panel.style.left = (panel.offsetLeft - pos1) + "px";
            panel.style.transform = "none";
            drawConnections();
        };
    };
}



async function updateActiveSensorsConf() {
    try {
        await fetch('/api/system/sensors', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sensors: activeSensors })
        });
    } catch(e) { console.error(e); }
}

async function startSystem() {
    try {
        const response = await fetch('/api/system/start', { method: 'POST' });
        const data = await response.json();
        console.log('[startSystem] Response:', data);
        
        
        if (!updateDataInterval) {
            updateData(); // call immediately first
            updateDataInterval = setInterval(updateData, 1000);
        }
    } catch(err) {
        console.error('[startSystem] Error:', err);
    }
}async function stopSystem() {
    try {
        console.log('[stopSystem] Sending stop request...');
        
        // stop polling immediately when system stops
        if (updateDataInterval) {
            clearInterval(updateDataInterval);
            updateDataInterval = null;
            console.log('[stopSystem] Polling stopped');
        }
        
        const response = await fetch('/api/system/stop', { method: 'POST' });
        const data = await response.json();
        console.log('[stopSystem] Response:', data);
        
        
        const sysStatus = document.getElementById('sys-status-text');
        sysStatus.textContent = 'Offline'; 
        sysStatus.style.color = '#ef4444';
    } catch(err) {
        console.error('[stopSystem] Error:', err);
    }
}

async function resetSystem() {
    try {
        
        if (updateDataInterval) {
            clearInterval(updateDataInterval);
            updateDataInterval = null;
        }
        
        
        const statusDiv = document.getElementById('system-status');
        if (statusDiv) {
            statusDiv.textContent = 'Offline';
            statusDiv.className = 'status offline';
        }
        
        
        const tickElement = document.getElementById('sys-tick');
        if (tickElement) {
            tickElement.textContent = '0';
        }
        
        const sensorTable = document.querySelector('#tbl-traffic-window tbody');
        if (sensorTable) {
            sensorTable.innerHTML = '';
        }
        
        const eventTable = document.querySelector('#tbl-event-window tbody');
        if (eventTable) {
            eventTable.innerHTML = '';
        }
        
        const commandTable = document.querySelector('#tbl-command-table tbody');
        if (commandTable) {
            commandTable.innerHTML = '';
        }
        
        const outputTable = document.querySelector('#tbl-output-buffer tbody');
        if (outputTable) {
            outputTable.innerHTML = '';
        }
        
        await fetch('/api/system/reset', { method: 'POST' });
        
        await new Promise(resolve => setTimeout(resolve, 500));
        
        const verifyRes = await fetch('/api/system/verify-reset');
        const verifyData = await verifyRes.json();
        
        console.log('[resetSystem] Verification:', verifyData);
        
        if (verifyData.status === 'success') {
            const verificationMsg = `
RESET VERIFICATION:
 sensor_stream: ${verifyData.sensor_stream_count} rows
 event_stream: ${verifyData.event_stream_count} rows
 command_stream: ${verifyData.command_stream_count} rows
 output_buffer: ${verifyData.output_buffer_count} rows
 system_tick: ${verifyData.current_tick}
${verifyData.all_cleared ? 'ALL TABLES CLEARED' : 'Some tables still have data'}`;
            console.log(verificationMsg);
            alert(verificationMsg);
        }
        
        // Restart polling to show updated (empty) data
        if (!updateDataInterval) {
            updateData(); // Call immediately first
            updateDataInterval = setInterval(updateData, 1000);
        }
    } catch(err) {
        console.error('[resetSystem] Error:', err);
        alert('Reset failed: ' + err.message);
    }
}

async function updateData() {
    try {
        // Get system status
        const statusRes = await fetch('/api/system/status');
        const statusData = await statusRes.json();
        console.log('[updateData] Status:', statusData);
        
        const sysStatus = document.getElementById('sys-status-text');
        if (statusData.system_running) {
            sysStatus.textContent = 'Running'; 
            sysStatus.style.color = '#10b981';
        } else {
            sysStatus.textContent = 'Offline'; 
            sysStatus.style.color = '#ef4444';
        }

        // Get current tick
        const tickRes = await fetch('/api/data/tick');
        const tickData = await tickRes.json();
        console.log('[updateData] Tick:', tickData);
        document.getElementById('sys-tick').textContent = tickData.tick || 0;

        // Determine which table is currently active
        const activeView = document.querySelector('.table-view.active');
        if (!activeView) {
            console.warn('[updateData] No active table view found');
            return;
        }
        
        const activeViewId = activeView.id;
        console.log('[updateData] Active view:', activeViewId);
        
        if (activeViewId === 'view-traffic-window') {
            const sensorRes = await fetch('/api/data/sensor-table');
            const sensorData = await sensorRes.json();
            console.log('[updateData] Sensor data:', sensorData);
            
            if (!sensorData.rows) {
                console.warn('[updateData] Sensor response missing rows property:', sensorData);
                return;
            }
            
            document.querySelector('#tbl-traffic-window tbody').innerHTML = sensorData.rows.map(r => 
                `<tr><td>${r.pkt_id}</td><td style="color:#a5b4fc;">${r.ts}</td><td>${r.road_id}</td><td>${r.speed.toFixed(1)}</td></tr>`
            ).join('');
        }
        else if (activeViewId === 'view-event-window') {
            const eventRes = await fetch('/api/data/event-table');
            const eventData = await eventRes.json();
            console.log('[updateData] Event data:', eventData);
            
            if (!eventData.rows) {
                console.warn('[updateData] Event response missing rows property:', eventData);
                return;
            }
            
            document.querySelector('#tbl-event-window tbody').innerHTML = eventData.rows.map(r => 
                `<tr><td>${r.evt_id}</td><td style="color:#a5b4fc;">${r.ts}</td><td>${r.road_id}</td><td>${r.event_type}</td></tr>`
            ).join('');
        }
        else if (activeViewId === 'view-command-table') {
            const cmdRes = await fetch('/api/data/command-table');
            const cmdData = await cmdRes.json();
            console.log('[updateData] Command data:', cmdData);
            
            if (!cmdData.rows) {
                console.warn('[updateData] Command response missing rows property:', cmdData);
                return;
            }
            
            document.querySelector('#tbl-command-table tbody').innerHTML = cmdData.rows.map(r => 
                `<tr><td>${r.cmd_id}</td><td style="color:#a5b4fc;">${r.ts}</td><td>${r.action}</td><td>${r.reason}</td></tr>`
            ).join('');
        }
        else if (activeViewId === 'view-output-buffer') {
            const outRes = await fetch('/api/data/output-buffer');
            const outData = await outRes.json();
            console.log('[updateData] Output buffer data:', outData);
            
            if (!Array.isArray(outData)) {
                console.warn('[updateData] Output buffer response is not an array:', outData);
                return;
            }
            
            document.querySelector('#tbl-output-buffer tbody').innerHTML = outData.map(r => 
                `<tr><td>${r.query_id}</td><td style="color:#a5b4fc;">${r.ts}</td><td>${JSON.stringify(r.parsed_result)}</td></tr>`
            ).join('');
        }
    } catch (err) {
        console.error('[updateData] Error:', err);
    }
}


async function loadSensors() {
    const container = document.querySelector('#sensor-panel .sensor-container');
    if (!container) return;

    container.innerHTML = '<div class="sensor-placeholder">Loading sensors...</div>';

    try {
        const res = await fetch('/api/sensors');
        if (!res.ok) {
            container.innerHTML = '<div class="no-sensors">Failed to load sensors</div>';
            console.error('Failed to fetch /api/sensors', await res.text());
            return;
        }
        const data = await res.json();
        container.innerHTML = ''; // clear

        if (!data.streams || data.streams.length === 0) {
            container.innerHTML = '<div class="no-sensors">No streams found in traffic.xml</div>';
            return;
        }

        const skipped = [];
        data.streams.forEach(s => {
            const rawName = (s.name || '').trim();
            if (!rawName) return;
            if (rawName.length > 20) { // enforce constraint
                skipped.push(rawName);
                return;
            }

            const card = document.createElement('div');
            card.className = 'sensor-card';
            card.setAttribute('draggable', 'true');
            // Pass the stream name as the drag type
            card.ondragstart = (ev) => dragStart(ev, rawName);
            card.dataset.type = rawName;

            // Display the sensor name (no SVG). Truncate for UI safety (shouldn't be >20 now).
            const displayName = rawName.length > 20 ? rawName.slice(0, 17) + '...' : rawName;
            card.innerHTML = `
                <div class="sensor-label" title="${escapeHtml(rawName)}">${escapeHtml(displayName)}</div>
            `;
            container.appendChild(card);
        });

        if (skipped.length) {
            const warn = document.createElement('div');
            warn.className = 'sensor-warn';
            warn.textContent = `${skipped.length} sensor(s) skipped: name length > 20 characters.`;
            container.appendChild(warn);
            console.warn('Skipped sensors (name > 20 chars):', skipped);
        }
    } catch (err) {
        container.innerHTML = '<div class="no-sensors">Error loading sensors</div>';
        console.error('Error loading sensors:', err);
    }
}

// small helper to avoid injection when setting innerHTML
function escapeHtml(str) {
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

/**
 * QUERY MANAGEMENT
 */

function openQueryModal() {
    document.getElementById('query-modal').classList.add('active');
    loadQueries();
}

function closeQueryModal() {
    document.getElementById('query-modal').classList.remove('active');
}

async function loadQueries() {
    try {
        await loadQueryMetadata();
        const response = await fetch('/api/queries');
        const data = await response.json();
        
        const queriesList = document.getElementById('queries-list');
        if (data.queries.length === 0) {
            queriesList.innerHTML = '<p class="query-empty-state">No queries defined yet</p>';
            return;
        }
        
        queriesList.innerHTML = data.queries.map(q => renderQueryCard(q)).join('');

        queriesList.querySelectorAll('.query-config-builder').forEach(builder => {
            const config = JSON.parse(builder.dataset.config || '{}');
            hydrateQueryBuilder(builder, config);
        });

        queriesList.querySelectorAll('[data-action="save"]').forEach(btn => {
            btn.addEventListener('click', () => updateQuery(btn.closest('.query-card').dataset.queryId));
        });
    } catch (err) {
        console.error('[loadQueries] Error:', err);
        document.getElementById('queries-list').innerHTML = '<p class="query-empty-state">Error loading queries</p>';
    }
}

async function loadQueryMetadata() {
    try {
        const response = await fetch('/api/queries/metadata');
        if (response.ok) {
            queryMetadata = await response.json();
        }
        const newBuilder = document.getElementById('new-query-builder');
        if (newBuilder && !newBuilder.dataset.ready) {
            hydrateQueryBuilder(newBuilder, defaultQueryConfig());
            newBuilder.dataset.ready = 'true';
        }
        updateGraphFieldOptions();
    } catch (err) {
        console.error('[loadQueryMetadata] Error:', err);
    }
}

function defaultQueryConfig() {
    return {
        source: 'sensor_stream',
        filter: { field: 'ts', operator: '=', value: '{CURRENT_TICK}' },
        group_by: ['road_id', 'intersection_id'],
        aggregations: [
            { name: 'avg_speed_avg', operation: 'avg', field: 'avg_speed' }
        ]
    };
}

function renderQueryCard(q) {
    const config = q.query_config || defaultQueryConfig();
    return `
        <div class="query-card" data-query-id="${escapeHtml(q.query_id)}">
            <div class="query-card-header">
                <span class="query-id-badge">${escapeHtml(q.query_id)}</span>
                <div class="query-card-actions">
                    <button class="query-action-btn query-save-btn" data-action="save">Save</button>
                </div>
            </div>
            <div class="query-meta-row">
                <span class="query-meta-pill">Every ${escapeHtml(q.frequency_sec)} tick(s)</span>
                <span class="query-meta-pill">${escapeHtml(config.source || '')}</span>
            </div>
            <div class="query-form-grid">
                <div class="form-group">
                    <label>Description</label>
                    <input type="text" data-field="description" value="${escapeHtml(q.description || '')}" placeholder="No description">
                </div>
                <div class="form-group">
                    <label>Frequency</label>
                    <input type="number" min="1" data-field="frequency_sec" value="${escapeHtml(q.frequency_sec)}">
                </div>
            </div>
            <div class="query-config-builder" data-config="${escapeHtml(JSON.stringify(config))}">
                <div class="query-form-grid">
                    <div class="form-group">
                        <label>Source Stream</label>
                        <select data-field="source"></select>
                    </div>
                    <div class="form-group">
                        <label>Filter</label>
                        <div class="filter-row">
                            <select data-field="filter-field"></select>
                            <select data-field="filter-operator">
                                <option value="=">=</option>
                                <option value="!=">!=</option>
                                <option value="<>">&lt;&gt;</option>
                                <option value=">">&gt;</option>
                                <option value=">=">&gt;=</option>
                                <option value="<">&lt;</option>
                                <option value="<=">&lt;=</option>
                            </select>
                            <input type="text" data-field="filter-value">
                        </div>
                    </div>
                </div>
                <div class="form-group">
                    <label>Group By</label>
                    <div class="group-by-options" data-field="group-by"></div>
                </div>
                <div class="aggregation-header">
                    <label>Aggregations</label>
                    <button type="button" class="query-action-btn query-save-btn" onclick="addAggregationRow(this.closest('.query-config-builder'))">Add Aggregation</button>
                </div>
                <div class="aggregation-list" data-field="aggregations"></div>
            </div>
        </div>
    `;
}

function sourceFields(source) {
    const sourceMeta = queryMetadata.sources.find(item => item.name === source) || queryMetadata.sources[0];
    return sourceMeta ? sourceMeta.fields : [];
}

function hydrateQueryBuilder(builder, config) {
    const sourceSelect = builder.querySelector('[data-field="source"]');
    sourceSelect.innerHTML = queryMetadata.sources.map(source =>
        `<option value="${escapeHtml(source.name)}">${escapeHtml(source.name)}</option>`
    ).join('');
    sourceSelect.value = config.source || queryMetadata.sources[0].name;
    sourceSelect.onchange = () => refreshBuilderFields(builder, collectQueryConfig(builder));
    refreshBuilderFields(builder, config);
}

function refreshBuilderFields(builder, config) {
    const source = builder.querySelector('[data-field="source"]').value;
    const fields = sourceFields(source);
    const groupBox = builder.querySelector('[data-field="group-by"]');
    const filterField = builder.querySelector('[data-field="filter-field"]');
    const filter = Array.isArray(config.filter) ? config.filter[0] : (config.filter || {});

    groupBox.innerHTML = ['road_id', 'intersection_id'].map(field => `
        <label class="field-check">
            <input type="checkbox" value="${escapeHtml(field)}" checked disabled>
            ${escapeHtml(field)}
        </label>
    `).join('');

    filterField.innerHTML = fields.map(field => `<option value="${escapeHtml(field)}">${escapeHtml(field)}</option>`).join('');
    filterField.value = filter.field || 'ts';
    builder.querySelector('[data-field="filter-operator"]').value = filter.operator || '=';
    builder.querySelector('[data-field="filter-value"]').value = filter.value || '{CURRENT_TICK}';

    const list = builder.querySelector('[data-field="aggregations"]');
    list.innerHTML = '';
    (config.aggregations || []).forEach(agg => addAggregationRow(builder, agg));
    if (!list.children.length) {
        addAggregationRow(builder, { name: 'avg_speed_avg', operation: 'avg', field: 'avg_speed' });
    }
}

function addAggregationRow(builder, agg = { name: '', operation: 'count', field: '*' }) {
    const source = builder.querySelector('[data-field="source"]').value || 'sensor_stream';
    const fields = source === 'sensor_stream'
        ? ['avg_speed', 'occupancy', 'vehicle_count']
        : sourceFields(source).filter(field => !['event_id', 'road_id', 'intersection_id', 'ts'].includes(field));
    const row = document.createElement('div');
    row.className = 'aggregation-row';
    row.innerHTML = `
        <select data-field="agg-field">
            ${fields.map(field => `<option value="${escapeHtml(field)}">${escapeHtml(field)}</option>`).join('')}
        </select>
        <select data-field="agg-operation">
            ${queryMetadata.operations.map(op => `<option value="${escapeHtml(op)}">${escapeHtml(op.toUpperCase())}</option>`).join('')}
        </select>
        <button type="button" class="query-action-btn query-delete-btn" onclick="this.closest('.aggregation-row').remove()">Remove</button>
    `;
    row.querySelector('[data-field="agg-operation"]').value = agg.operation || 'count';
    row.querySelector('[data-field="agg-field"]').value = agg.field || '*';
    builder.querySelector('[data-field="aggregations"]').appendChild(row);
}

function collectQueryConfig(builder) {
    const source = builder.querySelector('[data-field="source"]').value;
    const aggregations = Array.from(builder.querySelectorAll('.aggregation-row')).map(row => ({
        operation: row.querySelector('[data-field="agg-operation"]').value,
        field: row.querySelector('[data-field="agg-field"]').value
    })).map(agg => ({
        ...agg,
        name: `${agg.field === '*' ? 'all' : agg.field}_${agg.operation}`
    }));

    return {
        source,
        filter: {
            field: builder.querySelector('[data-field="filter-field"]').value,
            operator: builder.querySelector('[data-field="filter-operator"]').value,
            value: builder.querySelector('[data-field="filter-value"]').value.trim() || '{CURRENT_TICK}'
        },
        group_by: ['road_id', 'intersection_id'],
        dimension_fields: ['road_id', 'intersection_id'],
        aggregations
    };
}

async function addNewQuery() {
    try {
        const queryId = document.getElementById('new-query-id').value.trim();
        const description = document.getElementById('new-query-desc').value.trim();
        const frequencySec = parseInt(document.getElementById('new-query-freq').value) || 30;
        const queryConfig = collectQueryConfig(document.getElementById('new-query-builder'));
        
        if (!queryId || !queryConfig.aggregations.length) {
            alert('Query ID and at least one aggregation are required!');
            return;
        }
        
        const response = await fetch('/api/queries', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                query_id: queryId,
                description: description,
                query_config: queryConfig,
                frequency_sec: frequencySec
            })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            alert('Query added successfully!');
            // Clear form
            document.getElementById('new-query-id').value = '';
            document.getElementById('new-query-desc').value = '';
            document.getElementById('new-query-freq').value = '30';
            hydrateQueryBuilder(document.getElementById('new-query-builder'), defaultQueryConfig());
            // Reload queries list
            loadQueries();
        } else {
            alert('Error adding query: ' + data.message);
        }
    } catch (err) {
        console.error('[addNewQuery] Error:', err);
        alert('Error: ' + err.message);
    }
}

async function updateQuery(queryId) {
    const card = Array.from(document.querySelectorAll('.query-card'))
        .find(queryCard => queryCard.dataset.queryId === queryId);
    if (!card) return;

    const description = card.querySelector('[data-field="description"]').value.trim();
    const frequencySec = parseInt(card.querySelector('[data-field="frequency_sec"]').value, 10);
    const queryConfig = collectQueryConfig(card.querySelector('.query-config-builder'));

    if (!queryConfig.aggregations.length) {
        alert('At least one aggregation is required!');
        return;
    }
    if (!Number.isFinite(frequencySec) || frequencySec <= 0) {
        alert('Frequency must be greater than 0.');
        return;
    }

    try {
        const response = await fetch(`/api/queries/${encodeURIComponent(queryId)}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                description: description,
                query_config: queryConfig,
                frequency_sec: frequencySec
            })
        });

        const data = await response.json();

        if (response.ok) {
            alert('Query updated successfully!');
            loadQueries();
        } else {
            alert('Error updating query: ' + data.message);
        }
    } catch (err) {
        console.error('[updateQuery] Error:', err);
        alert('Error: ' + err.message);
    }
}

async function openGraphPanel() {
    document.getElementById('graph-panel').classList.add('active');
}

function closeGraphPanel() {
    document.getElementById('graph-panel').classList.remove('active');
    activeGraphConfig = null;
    if (graphRefreshInterval) {
        clearInterval(graphRefreshInterval);
        graphRefreshInterval = null;
    }
}

async function drawGranularGraph() {
    const stream = document.getElementById('graph-stream').value;
    const field = document.getElementById('graph-field').value;
    const metric = document.getElementById('graph-metric').value;
    const aggregate = document.getElementById('graph-aggregate').value;
    const fieldValue = document.getElementById('graph-field-value').value.trim();

    activeGraphConfig = { stream, field, metric, aggregate, fieldValue };
    await refreshGranularGraph();

    if (graphRefreshInterval) {
        clearInterval(graphRefreshInterval);
    }
    graphRefreshInterval = setInterval(refreshGranularGraph, 2000); 
}

async function refreshGranularGraph() {
    if (!activeGraphConfig) return;

    const params = new URLSearchParams({
        stream: activeGraphConfig.stream,
        field: activeGraphConfig.field,
        metric: activeGraphConfig.metric,
        aggregate: activeGraphConfig.aggregate,
        field_value: activeGraphConfig.fieldValue,
        window: '60'
    });

    try {
        const response = await fetch(`/api/graphs/granular?${params.toString()}`);
        const data = await response.json();

        if (!response.ok) {
            showGraphMessage(data.message || 'Unable to load graph data');
            return;
        }

        document.getElementById('graph-title').textContent = data.label || 'Granular Graph';
        if (!(data.points || []).length && Array.isArray(data.available) && data.available.length) {
            const examples = data.available
                .slice(0, 5)
                .map(item => `${item.field}=${item.field_value}, ${item.aggregate}(${item.metric})`)
                .join(' | ');
            showGraphMessage(`No exact match. Available: ${examples}`);
            return;
        }
        drawOutputGraph(data.points || [], `${activeGraphConfig.aggregate}(${activeGraphConfig.metric})`);
    } catch (err) {
        console.error('[refreshGranularGraph] Error:', err);
        showGraphMessage('Error loading graph data');
    }
}

function showGraphMessage(message) {
    const emptyState = document.getElementById('graph-empty-state');
    emptyState.textContent = message;
    emptyState.style.display = 'block';
    clearGraphCanvas();
}

function clearGraphCanvas() {
    const canvas = document.getElementById('graph-canvas');
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function drawOutputGraph(points, label) {
    const canvas = document.getElementById('graph-canvas');
    const ctx = canvas.getContext('2d');
    const emptyState = document.getElementById('graph-empty-state');
    clearGraphCanvas();

    if (!points.length) {
        emptyState.textContent = `No matching ${label} values found in the last 60s.`;
        emptyState.style.display = 'block';
        return;
    }

    emptyState.style.display = 'none';

    const width = canvas.width;
    const height = canvas.height;
    const padding = 42;
    const minTs = Math.min(...points.map(p => p.ts));
    const maxTs = Math.max(...points.map(p => p.ts));
    const values = points.map(p => p.value);
    const minY = Math.min(...values);
    const maxY = Math.max(...values);
    const tsRange = Math.max(1, maxTs - minTs);
    const yRange = Math.max(1, maxY - minY);

    ctx.strokeStyle = 'rgba(28,60,135,0.22)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(padding, padding);
    ctx.lineTo(padding, height - padding);
    ctx.lineTo(width - padding, height - padding);
    ctx.stroke();

    ctx.fillStyle = 'rgba(23,37,84,0.72)';
    ctx.font = '12px Segoe UI, sans-serif';
    ctx.fillText(String(Math.round(maxY * 100) / 100), 8, padding + 4);
    ctx.fillText(String(Math.round(minY * 100) / 100), 8, height - padding + 4);
    ctx.fillText(`t=${minTs}`, padding, height - 12);
    ctx.fillText(`t=${maxTs}`, width - padding - 42, height - 12);

    const coords = points.map(point => ({
        x: padding + ((point.ts - minTs) / tsRange) * (width - padding * 2),
        y: height - padding - ((point.value - minY) / yRange) * (height - padding * 2)
    }));

    ctx.strokeStyle = '#1c3c87';
    ctx.lineWidth = 3;
    ctx.beginPath();
    coords.forEach((point, index) => {
        if (index === 0) ctx.moveTo(point.x, point.y);
        else ctx.lineTo(point.x, point.y);
    });
    ctx.stroke();

    ctx.fillStyle = '#5584eb';
    coords.forEach(point => {
        ctx.beginPath();
        ctx.arc(point.x, point.y, 4, 0, Math.PI * 2);
        ctx.fill();
    });
}

async function deleteQuery(queryId) {
    if (!confirm(`Are you sure you want to delete query "${queryId}"?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/queries/${encodeURIComponent(queryId)}`, {
            method: 'DELETE'
        });
        
        const data = await response.json();
        
        if (response.ok) {
            alert('Query deleted successfully!');
            loadQueries();
        } else {
            alert('Error deleting query: ' + data.message);
        }
    } catch (err) {
        console.error('[deleteQuery] Error:', err);
        alert('Error: ' + err.message);
    }
}


function updateGraphFieldOptions() {
    const streamSelect = document.getElementById('graph-stream');
    const fieldSelect = document.getElementById('graph-field');
    const metricSelect = document.getElementById('graph-metric');
    if (!streamSelect || !fieldSelect || !metricSelect) return;

    const isEvent = streamSelect.value === 'event';
    const fields = isEvent ? ['event_type', 'road_id', 'intersection_id'] : ['road_id', 'intersection_id'];
    const metrics = isEvent ? ['priority', 'event_type'] : ['speed', 'occupancy', 'vehicle_count'];
    fieldSelect.innerHTML = fields.map(field => `<option value="${escapeHtml(field)}">${escapeHtml(field)}</option>`).join('');
    metricSelect.innerHTML = metrics.map(metric => `<option value="${escapeHtml(metric)}">${escapeHtml(metric)}</option>`).join('');

    const aggregateSelect = document.getElementById('graph-aggregate');
    if (aggregateSelect) {
        aggregateSelect.value = isEvent ? 'count' : 'min';
    }
}

document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.panel').forEach(makePanelDraggable);
    window.addEventListener('resize', drawConnections);
    
    const streamSelect = document.getElementById('graph-stream');
    if (streamSelect) {
        streamSelect.addEventListener('change', updateGraphFieldOptions);
    }
    
    if (typeof init === 'function') init(); 
    loadQueryMetadata();
    loadSensors();
    updateActiveSensorsConf();
    drawConnections();
    setTimeout(updateGraphFieldOptions, 200);
});
