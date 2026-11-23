"""Tests for the mobile app's Lambda web server (fetchers/app_web.py). No network/AWS."""

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fetchers"))
sys.modules.setdefault("boto3", mock.MagicMock())

import app_web  # noqa: E402

SECRET = "0123456789abcdef0123456789abcdef"


class Route(unittest.TestCase):
    def test_known_files_under_the_secret_path(self):
        self.assertEqual(app_web.route(f"/{SECRET}/", SECRET), (200, "index.html"))
        self.assertEqual(app_web.route(f"/{SECRET}/icon.svg", SECRET), (200, "icon.svg"))
        self.assertEqual(app_web.route(f"/{SECRET}/data/desk.json", SECRET), (200, "data/desk.json"))

    def test_bare_secret_redirects_to_the_folder(self):
        self.assertEqual(app_web.route(f"/{SECRET}", SECRET), (302, f"/{SECRET}/"))

    def test_everything_else_is_404(self):
        for path in ("/", "/index.html", "/wrong/", f"/{SECRET}/config/queries.json",
                     f"/{SECRET}/../config/queries.json", f"/{SECRET}x/"):
            self.assertEqual(app_web.route(path, SECRET)[0], 404, path)

    def test_no_secret_configured_serves_nothing(self):
        self.assertEqual(app_web.route("//", ""), (404, None))

    def test_handler_rejects_post(self):
        event = {"rawPath": f"/{SECRET}/", "requestContext": {"http": {"method": "POST"}}}
        with mock.patch.dict(os.environ, {"APP_PATH": SECRET}):
            self.assertEqual(app_web.handler(event, None)["statusCode"], 405)


if __name__ == "__main__":
    unittest.main()
