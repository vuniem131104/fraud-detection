from feast import FeatureService

from feature_views import transaction_features

fraud_detection_service = FeatureService(
    name="fraud_detection_service",
    features=[
        transaction_features,
    ],
)
