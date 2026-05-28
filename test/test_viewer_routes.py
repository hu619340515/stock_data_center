import unittest
from unittest.mock import Mock, patch

import viewer.server as server


class TestViewerRoutes(unittest.TestCase):
    def setUp(self):
        server.app.config["TESTING"] = True
        self.client = server.app.test_client()

    def test_stock_weekly_route_passes_plain_frequency(self):
        with patch.object(server, "_run_task") as run_task:
            run_task.return_value = (True, "ok")

            response = self.client.post("/api/aggregate_weekly", json={"target": "stock"})

            self.assertEqual(response.status_code, 200)
            func = run_task.call_args.args[0]
            pipeline = Mock()
            func(pipeline)
            pipeline.full_download_pipeline.assert_called_once_with("w")

    def test_stock_monthly_route_passes_plain_frequency(self):
        with patch.object(server, "_run_task") as run_task:
            run_task.return_value = (True, "ok")

            response = self.client.post("/api/aggregate_monthly", json={"target": "stock"})

            self.assertEqual(response.status_code, 200)
            func = run_task.call_args.args[0]
            pipeline = Mock()
            func(pipeline)
            pipeline.full_download_pipeline.assert_called_once_with("m")

    def test_etf_weekly_route_uses_etf_pipeline(self):
        with patch.object(server, "_run_task") as run_task:
            run_task.return_value = (True, "ok")

            response = self.client.post("/api/aggregate_weekly", json={"target": "etf"})

            self.assertEqual(response.status_code, 200)
            func = run_task.call_args.args[0]
            pipeline = Mock()
            func(pipeline)
            pipeline.etf_download_pipeline.assert_called_once_with("w")


if __name__ == "__main__":
    unittest.main()
