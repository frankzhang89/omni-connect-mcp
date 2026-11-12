// Contracts: official Nacos 1.x/2.x controllers and 3.x Admin APIs.
// Test controls affect this local stand-in only; the connector never calls them.
const http = require('http');
const identities = new Map();
let version = '2.4.3';
let stateStatus = null;
let namespaceStatus = 200;
let readStatus = 200;
let publishStatus = 200;
let requests = [];
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const input = req.method === 'POST' ? req.body : Object.fromEntries(url.searchParams);
  const args = typeof input === 'string' ? Object.fromEntries(new URLSearchParams(input)) : (input || {});
  const reply = (status, body, json = false) => {
    res.writeHead(status, { 'Content-Type': (json ? 'application/json' : 'text/plain') + ';charset=UTF-8' });
    res.end(json ? JSON.stringify(body) : body);
  };
  // @endpoint POST /__test__/configure
  if (req.method === 'POST' && url.pathname === '/__test__/configure') {
    version = args.version || '2.4.3';
    stateStatus = args.stateStatus == null ? null : Number(args.stateStatus);
    namespaceStatus = Number(args.namespaceStatus || 200);
    readStatus = Number(args.readStatus || 200);
    publishStatus = Number(args.publishStatus || 200);
    requests = [];
    return reply(200, { version }, true);
  }
  // @endpoint GET /__test__/requests
  if (url.pathname === '/__test__/requests') return reply(200, requests, true);
  requests.push({ method: req.method, path: url.pathname });
  const v3 = version.startsWith('3.');
  const legacy = version.startsWith('1.') || /^2\.[01]\./.test(version);
  // @endpoint GET /v3/admin/core/state
  if (req.method === 'GET' && url.pathname === '/v3/admin/core/state') {
    return reply(stateStatus || (v3 ? 200 : 404), { code: 0, data: { version } }, true);
  }
  // @endpoint GET /v1/console/server/state
  if (req.method === 'GET' && url.pathname === '/v1/console/server/state') {
    return reply(v3 ? 410 : 200, { version }, true);
  }
  // @endpoint POST /v1/auth/login
  // @endpoint POST /v3/auth/user/login
  if (req.method === 'POST' && url.pathname === (v3 ? '/v3/auth/user/login' : '/v1/auth/login')) {
    if (args.username !== 'user' || args.password !== 'password') return reply(403, 'invalid credentials');
    return reply(200, { accessToken: 'simulation-token', tokenTtl: 18000 }, true);
  }
  const token = v3 ? req.headers.accesstoken : args.accessToken;
  const namespacesPath = v3 ? '/v3/admin/core/namespace/list'
    : legacy ? '/v1/console/namespaces' : '/v2/console/namespace/list';
  // @endpoint GET /v1/console/namespaces
  // @endpoint GET /v2/console/namespace/list
  // @endpoint GET /v3/admin/core/namespace/list
  if (req.method === 'GET' && url.pathname === namespacesPath) {
    if (token !== 'simulation-token') return reply(403, 'invalid token');
    const publicCount = [...identities.values()].filter(([namespace]) => namespace === '').length;
    return reply(namespaceStatus, { code: legacy ? 200 : 0, data: [
      { namespace: v3 ? 'public' : '', namespaceShowName: 'public', configCount: publicCount },
      { namespace: 'empty-ns', namespaceShowName: 'Empty namespace', configCount: 0 }
    ] }, true);
  }
  const configPath = v3 ? '/v3/admin/cs/config' : '/v1/cs/configs';
  const listPath = v3 ? '/v3/admin/cs/config/list' : configPath;
  // @endpoint GET /v1/cs/configs
  // @endpoint POST /v1/cs/configs
  // @endpoint GET /v3/admin/cs/config
  // @endpoint POST /v3/admin/cs/config
  // @endpoint GET /v3/admin/cs/config/list
  if (url.pathname === configPath || url.pathname === listPath) {
    if (token !== 'simulation-token') return reply(403, 'invalid token');
    if (v3 && (args.accessToken != null || args.group != null || args.tenant != null || args.namespaceId == null)) {
      return reply(400, 'V3 identity fields and token header required');
    }
    const namespace = (v3 ? args.namespaceId : args.tenant) || '';
    const normalized = namespace === 'public' ? '' : namespace;
    const group = v3 ? args.groupName : args.group;
    if (req.method === 'GET' && readStatus !== 200) return reply(readStatus, 'read unavailable');
    if (req.method === 'GET' && url.pathname === listPath && args.search === 'accurate' && args.dataId === '' && group) {
      const pageNo = Number(args.pageNo);
      const pageSize = Number(args.pageSize);
      if (!Number.isInteger(pageNo) || pageNo < 1 || !Number.isInteger(pageSize) || pageSize < 1) {
        return reply(400, 'positive pageNo and pageSize required');
      }
      const matches = [...identities.values()].filter(([ns, g]) => ns === normalized && g === group);
      const offset = (pageNo - 1) * pageSize;
      const pageItems = matches.slice(offset, offset + pageSize).map(([, , dataId]) => ({ dataId }));
      const page = { totalCount: matches.length, pageItems };
      return reply(200, v3 ? { code: 0, data: page } : page, true);
    }
    if (!args.dataId || !group) return reply(400, 'dataId and group required');
    const identity = [normalized, group, args.dataId];
    const key = 'nacos.config.' + JSON.stringify(identity);
    if (req.method === 'GET' && url.pathname === configPath) {
      const value = await pm.state.get(key);
      if (value == null) return reply(404, 'not found');
      return v3 ? reply(200, { code: 0, data: { content: value.content } }, true) : reply(200, value.content);
    }
    if (req.method === 'POST' && url.pathname === configPath) {
      if (publishStatus !== 200) return reply(publishStatus, 'publication unavailable');
      if (typeof args.content !== 'string' || !['properties', 'yaml'].includes(args.type)) return reply(400, 'content/type required');
      await pm.state.set(key, { content: args.content, type: args.type });
      identities.set(key, identity);
      return v3 ? reply(200, { code: 0, data: true }, true) : reply(200, 'true');
    }
  }
  // Removed/mismatched generation endpoints are modeled as missing.
  if (/^\/v[123]\/(admin|console|cs|auth)\//.test(url.pathname)) return reply(404, 'wrong API generation');
  return reply(501, 'Endpoint not modeled');
}).listen(process.env.PORT || 14902);
