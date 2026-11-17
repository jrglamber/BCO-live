"""Place one registered read-only GET before BCO's legacy catch-all.

This touches FastAPI route dispatch only. It has no broker or execution authority.
"""

def promote_get(app, path, endpoint):
    routes = getattr(getattr(app, "router", None), "routes", None)
    if routes is None:
        raise RuntimeError("BCO FastAPI router unavailable")
    matches = [
        route for route in routes
        if getattr(route, "path", None) == path
        and getattr(route, "endpoint", None) is endpoint
        and "GET" in (getattr(route, "methods", set()) or set())
    ]
    if len(matches) != 1:
        raise RuntimeError(f"Expected one BCO GET endpoint {path}; found {len(matches)}")
    route = matches[0]
    routes.remove(route)
    routes.insert(0, route)
    if routes[0] is not route:
        raise RuntimeError("Could not prioritize BCO pause status endpoint")
    return {"ok": True, "route_index": 0, "path": path, "broker_writes": False}
