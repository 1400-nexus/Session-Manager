# Session-level observed loss %, at and above which the status display
# escalates the loss column's colour. Below WARNING it reads as healthy.
LOSS_WARNING_PCT = 5.0
LOSS_CRITICAL_PCT = 15.0

# How often the status display emits a `status` log line when there is no
# TTY to draw live tables on. Deliberately far slower than the live refresh:
# one greppable line every few seconds, not a full redraw twice a second
# that buries every other line in the captured log.
STATUS_LOG_INTERVAL_SECONDS = 5.0

# Mirrors file-monitor's SenderRegistry: three missed heartbeats, not one,
# so a single delayed heartbeat under load doesn't flap a receiver dead.
MISSED_HEARTBEAT_LIMIT = 3
HEARTBEAT_INTERVAL_SECONDS = 5.0
