"""BCO pause status must not be swallowed by the older root mount."""
import unittest
from fastapi import FastAPI
from bco_readonly_route_order import promote_get

class BcoPauseRouteTests(unittest.TestCase):
    def test_promote_broker_readonly_pause_above_legacy_catchall(self):
        app = FastAPI()
        app.mount("/", FastAPI())
        @app.get("/bco-live-entry-pause-v2")
        def status():
            return {"ok": True, "paused": True}
        result=promote_get(app,"/bco-live-entry-pause-v2",status)
        self.assertEqual(result["route_index"],0)
        self.assertFalse(result["broker_writes"])
        self.assertEqual(app.router.routes[0].path,"/bco-live-entry-pause-v2")
        self.assertEqual(app.router.routes[0].methods,{"GET"})
    def test_missing_route_raises(self):
        with self.assertRaises(RuntimeError):
            promote_get(FastAPI(),"/bco-live-entry-pause-v2",lambda:None)
    def test_preserves_other_routes(self):
        app=FastAPI()
        @app.get("/existing")
        def existing():
            return True
        @app.get("/bco-live-entry-pause-v2")
        def status():
            return True
        n=len(app.router.routes)
        promote_get(app,"/bco-live-entry-pause-v2",status)
        self.assertEqual(len(app.router.routes),n)
        self.assertIn("/existing",[x.path for x in app.router.routes])
if __name__=="__main__":
    unittest.main()
