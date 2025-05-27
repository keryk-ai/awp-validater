# The Cloud Functions for Firebase SDK to create Cloud Functions and set up triggers.
from firebase_functions import https_fn

# The Firebase Admin SDK to access Cloud Firestore.
from firebase_admin import initialize_app

# Import the Flask app
from app import app as flask_app

# Initialize Firebase Admin SDK
initialize_app()

# Expose the Flask app as a Cloud Function
@https_fn.on_request()
def app(req: https_fn.Request) -> https_fn.Response:
    with flask_app.request_context(req.environ):
        return flask_app.full_dispatch_request()