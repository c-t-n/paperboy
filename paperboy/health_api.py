from aiohttp import web


async def healthcheck(request):
    return web.json_response({"status": "ok"})


app = web.Application()
app.add_routes([web.get("/", healthcheck)])


async def run_api(host="0.0.0.0", port=8080):
    await web._run_app(app, host=host, port=port, access_log=None)
