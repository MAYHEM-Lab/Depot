const {createProxyMiddleware} = require('http-proxy-middleware');

module.exports = function(app) {
    app.use(
        createProxyMiddleware("/api", {
            target: 'http://localhost',
            changeOrigin: true,
        })
    );

    app.use(
        createProxyMiddleware("/upload", {
            target: 'http://localhost',
            changeOrigin: true,
        })
    );

    app.use(
        createProxyMiddleware("/notebook", {
            target: 'http://localhost',
            ws: true,
            changeOrigin: true,
        })
    );
};