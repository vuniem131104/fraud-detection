"""Feast entities — các "khoá" mà feature được gắn vào.

Bốn entity, mỗi entity một bảng feature ở offline store (``application.feat_*``) và
một không gian tên riêng trong Redis. Không có entity ghép (card+device): cặp
thẻ-thiết bị đã bỏ khỏi MVP — tín hiệu "thiết bị lạ" giờ đến từ
``device_age_hours`` và ``device_burst``.
"""

from feast import Entity, ValueType

user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.STRING,
              description="Khách hàng")
card = Entity(name="card", join_keys=["card_id"], value_type=ValueType.STRING,
              description="Thẻ thanh toán")
merchant = Entity(name="merchant", join_keys=["merchant_id"], value_type=ValueType.STRING,
                  description="Đơn vị chấp nhận thẻ")
device = Entity(name="device", join_keys=["device_id"], value_type=ValueType.STRING,
                description="Thiết bị (fingerprint)")
