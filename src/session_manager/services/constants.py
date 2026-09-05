# Session-level observed loss %, at and above which the status display
# escalates the loss column's colour. Below WARNING it reads as healthy.
LOSS_WARNING_PCT = 5.0
LOSS_CRITICAL_PCT = 15.0

# Mirrors file-monitor's SenderRegistry: three missed heartbeats, not one,
# so a single delayed heartbeat under load doesn't flap a receiver dead.
MISSED_HEARTBEAT_LIMIT = 3
HEARTBEAT_INTERVAL_SECONDS = 5.0
