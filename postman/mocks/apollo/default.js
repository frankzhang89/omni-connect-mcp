// Contract: apolloconfig/apollo docs/zh/client/other-language-client-user-guide.md.
// Portal contract: apolloconfig/apollo docs/zh/portal/apollo-open-api-platform.md.
// Synthetic data only; editing items and published releases have independent state.
const http = require('http');
const yaml = "# agent-converted\r\napp:\r\n  name: '订单'\r\n  enabled: 'true'\r\n";
const initial = { 'authen.merchantName': 'sqmallauth', 'service.url': 'https://api.example.com:8080/auth?enabled=true', 'other.key': 'unchanged' };
http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const reply = (status, body) => {
    res.writeHead(status, { 'Content-Type': 'application/json;charset=UTF-8' });
    res.end(JSON.stringify(body));
  };
  const portal = url.pathname.match(/^\/openapi\/v1\/envs\/([^/]+)\/apps\/([^/]+)\/clusters\/([^/]+)\/namespaces\/([^/]+)(.*)$/);
  // @endpoint GET /openapi/v1/envs/{env}/apps/{appId}/clusters/{cluster}/namespaces/{namespace}
  // @endpoint GET /openapi/v1/envs/{env}/apps/{appId}/clusters/{cluster}/namespaces/{namespace}/items/{key}
  // @endpoint PUT /openapi/v1/envs/{env}/apps/{appId}/clusters/{cluster}/namespaces/{namespace}/items/{key}
  // @endpoint GET /openapi/v1/envs/{env}/apps/{appId}/clusters/{cluster}/namespaces/{namespace}/releases/latest
  // @endpoint POST /openapi/v1/envs/{env}/apps/{appId}/clusters/{cluster}/namespaces/{namespace}/releases
  if (portal) {
    if (req.headers.authorization !== 'test-openapi-token') return reply(403, { message: 'denied' });
    const stateKey = 'apollo.namespace.' + JSON.stringify(portal.slice(1, 5).map(decodeURIComponent));
    const state = (await pm.state.get(stateKey)) || { draft: { ...initial }, active: { ...initial } };
    const suffix = portal[5];
    const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : (req.body || {});
    if (req.method === 'GET' && suffix === '') {
      return reply(200, { format: 'properties', items: Object.entries(state.draft).map(([key, value]) => ({ key, value })) });
    }
    if (suffix.startsWith('/items/')) {
      const key = decodeURIComponent(suffix.slice('/items/'.length));
      if (!Object.hasOwn(state.draft, key)) return reply(404, { message: 'item missing' });
      if (req.method === 'GET') return reply(200, { key, value: state.draft[key], comment: 'original comment',
        dataChangeLastModifiedBy: 'editor', dataChangeLastModifiedTime: '2026-10-26T10:00:00.000+0800' });
      if (req.method === 'PUT') {
        if (url.searchParams.get('createIfNotExists') !== 'false' || body.key !== key || typeof body.value !== 'string' || !body.dataChangeLastModifiedBy)
          return reply(400, { message: 'invalid item' });
        state.draft[key] = body.value;
        await pm.state.set(stateKey, state);
        return reply(200, {});
      }
    }
    if (req.method === 'GET' && suffix === '/releases/latest') return reply(200, { configurations: state.active });
    if (req.method === 'POST' && suffix === '/releases') {
      if (!body.releaseTitle || !body.releasedBy) return reply(400, { message: 'invalid release' });
      state.active = { ...state.draft };
      await pm.state.set(stateKey, state);
      return reply(200, { configurations: state.active });
    }
    return reply(501, { message: 'Endpoint not modeled' });
  }
  // @endpoint GET /services/config
  if (req.method === 'GET' && url.pathname === '/services/config') {
    // Path routing strips /apollo before forwarding. Advertise the public simulation URL.
    return reply(200, [{ homepageUrl: process.env.APOLLO_SIM_PUBLIC_URL || 'http://127.0.0.1:14900/apollo' }]);
  }
  // @endpoint GET /configs/{appId}/{cluster}/{namespace}
  if (req.method === 'GET' && /^\/configs\/[^/]+\/[^/]+\/[^/]+$/.test(url.pathname)) {
    const namespace = decodeURIComponent(url.pathname.split('/').pop());
    if (namespace === 'missing') return reply(404, { message: 'namespace has no release' });
    if (namespace === 'denied') return reply(401, { message: 'invalid signature' });
    const parts = url.pathname.split('/').slice(2).map(decodeURIComponent);
    const state = await pm.state.get('apollo.namespace.' + JSON.stringify(['DEV', ...parts]));
    const configurations = state ? state.active : /\.ya?ml$/.test(namespace)
      ? { content: yaml } : { 'app.name': '订单', 'app.enabled': 'true', 'app.code': '00123' };
    return reply(200, { appId: 'source-app', cluster: 'default', namespaceName: namespace, configurations, releaseKey: 'release-1' });
  }
  return reply(501, { message: 'Endpoint not modeled' });
}).listen(process.env.PORT || 14901);
