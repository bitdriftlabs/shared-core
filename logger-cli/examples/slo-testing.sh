
#!/bin/bash

# =============================================================================
# SLO Alert Testing Script using bitdrift logger-cli
# =============================================================================
#
# This script simulates network request logs to test SLO alerting functionality.
# 
# SLO Alerting Background:
# - SLOs operate on RATE charts (success rate = successful events / total events)
# - SLOs use MWMBR (Multi-Window/Multi-Burn Rate) alerting
# - Alert fires when burn rate threshold is exceeded in BOTH long AND short windows
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ DEFAULT 7-DAY SLO CONFIGURATION (from SLODataTable.tsx)                     │
# ├─────────────┬──────────────┬────────────┬───────────┬──────────────────────┤
# │ Long Window │ Short Window │ Burn Rate  │ Err Budget│ Alert Fires When     │
# ├─────────────┼──────────────┼────────────┼───────────┼──────────────────────┤
# │ 1 hour      │ 5 minutes    │ 16.8       │ 10%       │ error > 1.68%        │
# │ 6 hours     │ 30 minutes   │ 5.6        │ 20%       │ error > 0.56%        │
# │ 24 hours    │ 2 hours      │ 2.8        │ 40%       │ error > 0.28%        │
# └─────────────┴──────────────┴────────────┴───────────┴──────────────────────┘
#
# SLO Target: 99.9% success rate
# Error Budget: 0.1% (= 1 - 99.9%)
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ HOW ALERTS FIRE (from slo_evaluator.rs)                                     │
# ├─────────────────────────────────────────────────────────────────────────────┤
# │ burn_rate_threshold = burn_rate × (1 - slo_target)                          │
# │                     = burn_rate × 0.001                                     │
# │                                                                             │
# │ Example: burn_rate=16.8 → 16.8 × 0.001 = 0.0168 (1.68%)                     │
# │                                                                             │
# │ Alert fires when error_rate > burn_rate_threshold in BOTH windows           │
# ├─────────────────────────────────────────────────────────────────────────────┤
# │ HOW TO TRIGGER EACH THRESHOLD INDEPENDENTLY                                 │
# ├─────────────────────────────────────────────────────────────────────────────┤
# │ "Alert fires when" values: #1=1.68% > #2=0.56% > #4=0.30% > #3=0.28%        │
# │                                                                             │
# │ To trigger ONLY one threshold, error rate must be:                          │
# │   - ABOVE that threshold's "fires when" value                               │
# │   - BELOW all higher "fires when" values                                    │
# │   - Run long enough to fill that threshold's SHORT window                   │
# │   - But NOT long enough to fill larger SHORT windows                        │
# │                                                                             │
# │ ONLY #1 (1hr/5min): error > 1.68%, run ~7 min                               │
# │ ONLY #2 (6hr/30min): 0.56% < error < 1.68%, run ~35 min                     │
# │ ONLY #3 (1d/2hr): 0.28% < error < 0.30%, run ~2.5 hr                        │
# │ ONLY #4 (4hr/5min): 0.30% < error < 0.56%, run ~7 min                       │
# │                                                                             │
# │ ALERT RESOLUTION TIME:                                                      │
# │   Alert resolves when LONG window no longer exceeds burn_rate_threshold.    │
# │   Your observation: 16:47→18:57 = 2hr 10min matches the 2hr long window.    │
# └─────────────────────────────────────────────────────────────────────────────┘
#
# ┌─────────────────────────────────────────────────────────────────────────────┐
# │ HOW SLO ALERTS FIRE (from loop-api slo_evaluator.rs)                        │
# ├─────────────────────────────────────────────────────────────────────────────┤
# │ error_threshold = 1.0 - slo_target                                          │
# │ burn_rate_threshold = burn_rate × error_threshold                           │
# │                                                                             │
# │ Alert fires when BOTH:                                                      │
# │   short_error > burn_rate_threshold  AND                                    │
# │   long_error > burn_rate_threshold                                          │
# │                                                                             │
# │ Example (99.9% SLO, 16.8 burn rate):                                        │
# │   error_threshold = 1.0 - 0.999 = 0.001 (0.1%)                              │
# │   burn_rate_threshold = 16.8 × 0.001 = 0.0168 (1.68%)                       │
# │   Alert fires when error rate > 1.68% in BOTH windows                       │
# │                                                                             │
# │ Example (99% SLO, 14.4 burn rate):                                          │
# │   error_threshold = 1.0 - 0.99 = 0.01 (1%)                                  │
# │   burn_rate_threshold = 14.4 × 0.01 = 0.144 (14.4%)                         │
# │   Alert fires when error rate > 14.4% in BOTH windows                       │
# └─────────────────────────────────────────────────────────────────────────────┘
#
# IMPORTANT: Each WindowAndBurnRate is evaluated INDEPENDENTLY!
# If ANY threshold is breached in BOTH its windows, that threshold fires.
# Multiple thresholds CAN fire simultaneously if error rate exceeds all of them.
#
# =============================================================================

set -e

# Configuration
LOGGER_HOST="${LOGGER_HOST:-localhost}"
LOGGER_PORT="${LOGGER_PORT:-5501}"
API_KEY="${API_KEY:-your-api-key-here}"
API_URL="${API_URL:-https://api.bitdrift.io}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logger CLI path - adjust if needed
LOGGER_CLI="${LOGGER_CLI:-logger-cli}"

print_header() {
    echo ""
    echo -e "${BLUE}=================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=================================================================${NC}"
}

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if logger-cli server is running
check_server() {
    if ! nc -z "$LOGGER_HOST" "$LOGGER_PORT" 2>/dev/null; then
        print_error "Logger server not running on $LOGGER_HOST:$LOGGER_PORT"
        echo ""
        echo "Start the server first with:"
        echo "  cd /Users/mguena/Desktop/shared-core"
        echo "  SKIP_PROTO_GEN=1 cargo run -p logger-cli -- start --api-key \$API_KEY"
        echo ""
        echo "Or set environment variables:"
        echo "  export API_KEY=your-bitdrift-api-key"
        echo "  export LOGGER_HOST=localhost"
        echo "  export LOGGER_PORT=5501"
        exit 1
    fi
    print_info "Logger server is running on $LOGGER_HOST:$LOGGER_PORT"
}

# Log a Network Response event (matches "Default Events > Network Response")
# Fields based on: libs/explorations/feature-shell/src/lib/workflows/rules/RuleOutOfTheBox/events/mobile/common.ts
# IMPORTANT: The "os" field must match the case expected by the matcher ("Android" not "android")
log_network_response() {
    local status_code="$1"   # HTTP status code: 200, 500, etc.
    local result="$2"        # "success", "failure", or "canceled"
    local host="$3"
    local path="$4"
    local duration_ms="$5"
    
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type span \
        --log-level info \
        --field "os" "Android" \
        --field "_status_code" "$status_code" \
        --field "_result" "$result" \
        --field "_host" "$host" \
        --field "_path" "$path" \
        --field "_path_template" "$path" \
        --field "_duration_ms" "$duration_ms" \
        --field "_method" "GET" \
        --field "_request_body_bytes_sent_count" "0" \
        --field "_response_body_bytes_received_count" "1024" \
        "HTTPResponse"
}

# Log a network request span (start + stop)
log_network_request() {
    local url="$1"
    local status_code="$2"
    local success="$3"
    local latency_ms="$4"
    local span_id="$5"
    
    # Start span
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type span \
        --log-level info \
        --field "_span_id" "$span_id" \
        --field "_span_type" "start" \
        --field "url" "$url" \
        --field "method" "GET" \
        "network_request_start"
    
    # Simulate latency
    sleep "$(echo "scale=3; $latency_ms/1000" | bc)"
    
    # Stop span with result
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type span \
        --log-level info \
        --field "_span_id" "$span_id" \
        --field "_span_type" "stop" \
        --field "url" "$url" \
        --field "status_code" "$status_code" \
        --field "success" "$success" \
        --field "latency_ms" "$latency_ms" \
        "network_request_complete"
}

# Log a simple success/failure event (for rate charts)
log_rate_event() {
    local event_type="$1"   # "total" or "success"
    local category="$2"      # e.g., "network_request", "app_start"
    local details="$3"
    
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type normal \
        --log-level info \
        --field "event_type" "$event_type" \
        --field "category" "$category" \
        --field "details" "$details" \
        "${category}_${event_type}"
}

# ============================================================================
# TEST SCENARIO 1: Baseline healthy traffic (99.9% success rate)
# ============================================================================
scenario_healthy_baseline() {
    print_header "SCENARIO 1: Healthy Baseline (99.9% success rate)"
    print_info "Simulating normal traffic pattern - 1000 requests, ~1 failure"
    print_info "This should NOT trigger any SLO alerts"
    
    local total=1000
    local failures=1
    local successes=$((total - failures))
    
    for i in $(seq 1 $total); do
        if [ $i -le $failures ]; then
            # Failure
            log_network_response "500" "failure" "api.example.com" "/data" "250"
            print_warning "[$i/$total] Logged FAILURE"
        else
            # Success
            log_network_response "200" "success" "api.example.com" "/data" "50"
        fi
        
        # Progress indicator
        if [ $((i % 100)) -eq 0 ]; then
            print_info "Progress: $i/$total requests logged"
        fi
        
        # Small delay between requests
        sleep 0.05
    done
    
    print_info "Scenario 1 complete: $successes successes, $failures failures"
    print_info "Success rate: $(echo "scale=2; $successes * 100 / $total" | bc)%"
}

# ============================================================================
# TEST SCENARIO 2: High burn rate - triggers ALL thresholds (5% error rate)
# ============================================================================
scenario_high_burn_rate_1hr() {
    print_header "SCENARIO 2: High Burn Rate - Triggers ALL Thresholds"
    echo ""
    print_info "┌─────────────────────────────────────────────────────────────────┐"
    print_info "│ This test uses 5% error rate - EXCEEDS ALL thresholds!          │"
    print_info "│                                                                 │"
    print_info "│ Your thresholds (99.9% SLO):                                    │"
    print_info "│   #1 (1hr/5min):  1.68% → 5% > 1.68% ✅ FIRES                   │"
    print_info "│   #2 (6hr/30min): 0.56% → 5% > 0.56% ✅ FIRES                   │"
    print_info "│   #3 (1d/2hr):    0.28% → 5% > 0.28% ✅ FIRES                   │"
    print_info "│   #4 (4hr/5min):  0.30% → 5% > 0.30% ✅ FIRES                   │"
    print_info "│                                                                 │"
    print_info "│ Use Scenarios T1, T2, T3, T4 for targeted threshold testing     │"
    print_info "└─────────────────────────────────────────────────────────────────┘"
    echo ""
    
    local duration_seconds=360   # 6 minutes
    local failure_rate=5         # 5% failure rate
    
    print_warning "Configuration:"
    print_warning "  Duration: ${duration_seconds}s (6 minutes)"
    print_warning "  Failure rate: ${failure_rate}%"
    print_warning "  This exceeds ALL thresholds for 99.9% SLO"
    echo ""
    
    run_burn_rate_test $duration_seconds $failure_rate
}

# ============================================================================
# TEST SCENARIO T1: Only trigger threshold #1 (1hr/5min, 16.8 burn rate)
# ============================================================================
scenario_trigger_threshold_1() {
    print_header "SCENARIO T1: Only Threshold #1 (1hr/5min window)"
    echo ""
    print_info "┌─────────────────────────────────────────────────────────────────┐"
    print_info "│ TARGET: Threshold #1 (1hr long / 5min short, burn rate 16.8)    │"
    print_info "├─────────────────────────────────────────────────────────────────┤"
    print_info "│ SLO Target: 99.9% → Error Budget: 0.1%                          │"
    print_info "│ Alert fires when: error > 16.8 × 0.1% = 1.68%                   │"
    print_info "│                                                                 │"
    print_info "│ Strategy: Use 10% error rate for 6 minutes                      │"
    print_info "│   - 10% >> 1.68% ✅ Will definitely fire threshold #1           │"
    print_info "│   - 6 min fills the 5-min short window                          │"
    print_info "│   - 6 min does NOT fill 30-min window (threshold #2)            │"
    print_info "│                                                                 │"
    print_info "│ RESOLUTION TIME: ~1 hour (long window duration)                 │"
    print_info "└─────────────────────────────────────────────────────────────────┘"
    echo ""
    
    local duration_seconds=360   # 6 minutes
    local failure_rate=10        # 10% error rate (proven to work)
    
    print_warning "Configuration:"
    print_warning "  Duration: 6 minutes (fills 5min window)"
    print_warning "  Error rate: 10% (fires when > 1.68%)"
    print_warning "  Rate: ~3 RPS"
    print_warning "  Expected: ONLY threshold #1 fires"
    print_warning "  Resolution: ~1 hour after test ends"
    echo ""
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local count=0
    local failures=0
    local successes=0
    local delay="0.33"  # ~3 RPS
    
    print_info "Starting at $(date '+%H:%M:%S'), will run until $(date -r $end_time '+%H:%M:%S')"
    
    while [ $(date +%s) -lt $end_time ]; do
        count=$((count + 1))
        
        local rand=$((RANDOM % 100))
        if [ $rand -lt $failure_rate ]; then
            log_network_response "503" "failure" "api.example.com" "/critical" "5000"
            failures=$((failures + 1))
        else
            log_network_response "200" "success" "api.example.com" "/critical" "45"
            successes=$((successes + 1))
        fi
        
        local elapsed=$(($(date +%s) - start_time))
        if [ $((count % 180)) -eq 0 ]; then
            local current_error_rate=$(echo "scale=1; $failures * 100 / $count" | bc)
            print_info "[${elapsed}s] Sent: $count | Failures: $failures | Error rate: ${current_error_rate}%"
        fi
        
        sleep $delay
    done
    
    local total_time=$(($(date +%s) - start_time))
    local final_error_rate=$(echo "scale=2; $failures * 100 / $count" | bc)
    
    echo ""
    print_header "Test Complete!"
    print_info "Duration: ${total_time}s | Total: $count | Failures: $failures | Error rate: ${final_error_rate}%"
    print_warning "✅ Check your SLO alerts - threshold #1 should fire!"
}

# ============================================================================
# TEST SCENARIO T2: Trigger threshold #2 (6hr/30min, 5.6 burn rate)
# ============================================================================
# DEFAULT 7-DAY SLO THRESHOLDS (from SLODataTable.tsx):
#   #1: 1hr/5min,   burn 16.8, fires at > 1.68%
#   #2: 6hr/30min,  burn 5.6,  fires at > 0.56%
#   #3: 24hr/2hr,   burn 2.8,  fires at > 0.28%
#
# To fire #2 but NOT #1: need 0.56% < error < 1.68%
# NOTE: #3 will ALSO fire since any rate > 0.56% is also > 0.28%
# ============================================================================
scenario_trigger_threshold_2() {
    print_header "SCENARIO T2: Threshold #2 (6hr/30min window)"
    echo ""
    print_info "┌─────────────────────────────────────────────────────────────────┐"
    print_info "│ TARGET: Threshold #2 (6hr long / 30min short, burn rate 5.6)    │"
    print_info "├─────────────────────────────────────────────────────────────────┤"
    print_info "│ DEFAULT 7-DAY SLO CONFIG:                                       │"
    print_info "│   #1: 1hr/5min,  burn 16.8 → fires at > 1.68%                   │"
    print_info "│   #2: 6hr/30min, burn 5.6  → fires at > 0.56%  ← TARGET         │"
    print_info "│   #3: 24hr/2hr,  burn 2.8  → fires at > 0.28%                   │"
    print_info "│                                                                 │"
    print_info "│ Strategy: Use 1.5% error rate for 35 minutes                    │"
    print_info "│   - 1.5% > 0.56% ✅ Will fire threshold #2                      │"
    print_info "│   - 1.5% < 1.68% ❌ Will NOT fire threshold #1                  │"
    print_info "│   - 1.5% > 0.28% ✅ Will ALSO fire threshold #3                 │"
    print_info "│   - 35 min fills the 30-min short window                        │"
    print_info "│                                                                 │"
    print_info "│ EXPECTED: #2 and #3 fire, but NOT #1                            │"
    print_info "│ ⚠️  This test runs 35 minutes!                                  │"
    print_info "│ RESOLUTION TIME: ~6 hours (long window duration)                │"
    print_info "└─────────────────────────────────────────────────────────────────┘"
    echo ""
    
    local duration_seconds=2100  # 35 minutes
    local failure_rate=1         # 1% error rate - between 0.56% and 1.68%
    
    print_warning "Configuration:"
    print_warning "  Duration: 35 minutes (fills 30min window)"
    print_warning "  Error rate: ~1% (above 0.56%, below 1.68%)"
    print_warning "  Rate: ~5 RPS"
    print_warning "  Expected: #2 and #3 fire (NOT #1)"
    print_warning "  Resolution: ~6 hours after test ends"
    echo ""
    
    read -p "This test runs 35 minutes. Continue? (y/N): " confirm
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
        print_info "Cancelled."
        return
    fi
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local count=0
    local failures=0
    local successes=0
    local delay="0.2"  # ~5 RPS for more data
    
    print_info "Starting at $(date '+%H:%M:%S'), will run until $(date -r $end_time '+%H:%M:%S')"
    
    while [ $(date +%s) -lt $end_time ]; do
        count=$((count + 1))
        
        local rand=$((RANDOM % 100))
        if [ $rand -lt $failure_rate ]; then
            log_network_response "503" "failure" "api.example.com" "/critical" "5000"
            failures=$((failures + 1))
        else
            log_network_response "200" "success" "api.example.com" "/critical" "45"
            successes=$((successes + 1))
        fi
        
        local elapsed=$(($(date +%s) - start_time))
        if [ $((count % 300)) -eq 0 ]; then  # Update every minute
            local current_error_rate=$(echo "scale=2; $failures * 100 / $count" | bc)
            local remaining=$((duration_seconds - elapsed))
            local remaining_min=$((remaining / 60))
            print_info "[${elapsed}s] Sent: $count | Failures: $failures | Error rate: ${current_error_rate}% | Remaining: ${remaining_min}min"
        fi
        
        sleep $delay
    done
    
    local total_time=$(($(date +%s) - start_time))
    local final_error_rate=$(echo "scale=2; $failures * 100 / $count" | bc)
    
    echo ""
    print_header "Test Complete!"
    print_info "Duration: ${total_time}s | Total: $count | Failures: $failures | Error rate: ${final_error_rate}%"
    print_warning "Expected results:"
    print_warning "  ❌ #1 (1hr/5min)  - should NOT fire (${final_error_rate}% < 1.68%)"
    print_warning "  ✅ #2 (6hr/30min) - SHOULD fire (${final_error_rate}% > 0.56%)"
    print_warning "  ✅ #3 (24hr/2hr)  - SHOULD fire (${final_error_rate}% > 0.28%)"
}

# ============================================================================
# TEST SCENARIO T3: Only trigger threshold #3 (1d/2hr, 2.8 burn rate)
# ============================================================================
scenario_trigger_threshold_3() {
    print_header "SCENARIO T3: Only Threshold #3 (1day/2hr window)"
    echo ""
    print_info "┌─────────────────────────────────────────────────────────────────┐"
    print_info "│ TARGET: Threshold #3 (1day long / 2hr short, burn rate 2.8)     │"
    print_info "├─────────────────────────────────────────────────────────────────┤"
    print_info "│ Alert fires when error > 2.8 × 0.1% = 0.28%                     │"
    print_info "│ Threshold #4 fires when error > 0.30%                           │"
    print_info "│                                                                 │"
    print_info "│ ⚠️  VERY NARROW WINDOW: Need 0.28% < error < 0.30%              │"
    print_info "│     This is only a 0.02% difference!                            │"
    print_info "│                                                                 │"
    print_info "│ Strategy: Use 0.29% error rate for 2.5 hours                    │"
    print_info "│   - 0.29% > 0.28% ✅ Will fire threshold #3                     │"
    print_info "│   - 0.29% < 0.30% ❌ Will NOT fire threshold #4                 │"
    print_info "│   - 2.5 hr fills the 2hr short window                           │"
    print_info "│                                                                 │"
    print_info "│ ⚠️  This test runs 2.5 HOURS!                                   │"
    print_info "│ ⚠️  Very difficult due to tight tolerance                       │"
    print_info "│ RESOLUTION TIME: ~1 day (long window duration)                  │"
    print_info "└─────────────────────────────────────────────────────────────────┘"
    echo ""
    
    local duration_seconds=9000  # 2.5 hours = 150 minutes
    
    print_warning "Configuration:"
    print_warning "  Duration: 2.5 hours (fills 2hr window)"
    print_warning "  Error rate: ~0.29% (above 0.28%, below 0.30%)"
    print_warning "  Rate: ~3 RPS"
    print_warning "  Expected: ONLY threshold #3 fires (NOT #4)"
    print_warning "  Resolution: ~1 day after test ends"
    echo ""
    print_error "⚠️  THIS IS A 2.5 HOUR TEST WITH VERY TIGHT TOLERANCE!"
    print_error "⚠️  Due to the narrow range (0.28%-0.30%), results may vary"
    
    read -p "This test runs 2.5 HOURS. Continue? (y/N): " confirm
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
        print_info "Cancelled."
        return
    fi
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local count=0
    local failures=0
    local successes=0
    local delay="0.33"  # ~3 RPS
    
    print_info "Starting at $(date '+%H:%M:%S'), will run until $(date -r $end_time '+%H:%M:%S')"
    
    while [ $(date +%s) -lt $end_time ]; do
        count=$((count + 1))
        
        # Use modulo 10000 for 0.29% precision (29 per 10000)
        local rand=$((RANDOM % 10000))
        if [ $rand -lt 29 ]; then
            log_network_response "503" "failure" "api.example.com" "/critical" "5000"
            failures=$((failures + 1))
        else
            log_network_response "200" "success" "api.example.com" "/critical" "45"
            successes=$((successes + 1))
        fi
        
        local elapsed=$(($(date +%s) - start_time))
        if [ $((count % 540)) -eq 0 ]; then  # Every 3 minutes
            local current_error_rate=$(echo "scale=3; $failures * 100 / $count" | bc)
            local remaining=$((duration_seconds - elapsed))
            local remaining_min=$((remaining / 60))
            print_info "[${elapsed}s] Sent: $count | Failures: $failures | Error rate: ${current_error_rate}% | Remaining: ${remaining_min}min"
        fi
        
        sleep $delay
    done
    
    local total_time=$(($(date +%s) - start_time))
    local final_error_rate=$(echo "scale=3; $failures * 100 / $count" | bc)
    
    echo ""
    print_header "Test Complete!"
    print_info "Duration: ${total_time}s | Total: $count | Failures: $failures | Error rate: ${final_error_rate}%"
    print_warning "✅ Check your SLO alerts - threshold #3 should fire (NOT #4)!"
}

# ============================================================================
# TEST SCENARIO T4: Only trigger threshold #4 (4hr/5min, 3.0 burn rate)
# ============================================================================
scenario_trigger_threshold_4() {
    print_header "SCENARIO T4: Only Threshold #4 (4hr/5min window)"
    echo ""
    print_info "┌─────────────────────────────────────────────────────────────────┐"
    print_info "│ TARGET: Threshold #4 (4hr long / 5min short, burn rate 3.0)     │"
    print_info "├─────────────────────────────────────────────────────────────────┤"
    print_info "│ Alert fires when error > 3.0 × 0.1% = 0.30%                     │"
    print_info "│ Threshold #2 fires when error > 0.56%                           │"
    print_info "│                                                                 │"
    print_info "│ ⚠️  PROBLEM: Need 0.30% < error < 0.56% - very narrow range!    │"
    print_info "│                                                                 │"
    print_info "│ Strategy: Use 0.4% error rate for 6 minutes                     │"
    print_info "│   - 0.4% > 0.30% ✅ Will fire threshold #4                      │"
    print_info "│   - 0.4% < 0.56% ❌ Will NOT fire threshold #2                  │"
    print_info "│   - 6 min fills the 5-min short window                          │"
    print_info "│   - 6 min does NOT fill 30-min window                           │"
    print_info "│                                                                 │"
    print_info "│ NOTE: This requires very precise error rate control             │"
    print_info "│ RESOLUTION TIME: ~4 hours (long window duration)                │"
    print_info "└─────────────────────────────────────────────────────────────────┘"
    echo ""
    
    local duration_seconds=360   # 6 minutes
    # Need 0.4% = 4 failures per 1000 requests
    # At 3 RPS for 6 min = ~1080 requests, need ~4 failures
    
    print_warning "Configuration:"
    print_warning "  Duration: 6 minutes (fills 5min window)"
    print_warning "  Error rate: ~0.4% (above 0.30%, below 0.56%)"
    print_warning "  Rate: ~3 RPS"
    print_warning "  Expected: ONLY threshold #4 fires (NOT #2)"
    print_warning "  Resolution: ~4 hours after test ends"
    echo ""
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local count=0
    local failures=0
    local successes=0
    local delay="0.33"  # ~3 RPS
    
    print_info "Starting at $(date '+%H:%M:%S'), will run until $(date -r $end_time '+%H:%M:%S')"
    
    while [ $(date +%s) -lt $end_time ]; do
        count=$((count + 1))
        
        # Use modulo 1000 for 0.4% precision (4 per 1000)
        local rand=$((RANDOM % 1000))
        if [ $rand -lt 4 ]; then
            log_network_response "503" "failure" "api.example.com" "/critical" "5000"
            failures=$((failures + 1))
        else
            log_network_response "200" "success" "api.example.com" "/critical" "45"
            successes=$((successes + 1))
        fi
        
        local elapsed=$(($(date +%s) - start_time))
        if [ $((count % 180)) -eq 0 ]; then
            local current_error_rate=$(echo "scale=2; $failures * 100 / $count" | bc)
            print_info "[${elapsed}s] Sent: $count | Failures: $failures | Error rate: ${current_error_rate}%"
        fi
        
        sleep $delay
    done
    
    local total_time=$(($(date +%s) - start_time))
    local final_error_rate=$(echo "scale=2; $failures * 100 / $count" | bc)
    
    echo ""
    print_header "Test Complete!"
    print_info "Duration: ${total_time}s | Total: $count | Failures: $failures | Error rate: ${final_error_rate}%"
    print_warning "✅ Check your SLO alerts - threshold #4 should fire (NOT #2)!"
}

# ============================================================================
# Helper function for precise burn rate tests (sub-1% error rates)
# ============================================================================
run_precise_burn_rate_test() {
    local duration_seconds=$1
    local failure_rate_per_10k=$2  # e.g., 40 = 0.4%, 29 = 0.29%
    local delay="0.2"  # 5 RPS
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local count=0
    local failures=0
    local successes=0
    
    print_info "Starting at $(date '+%H:%M:%S'), will run until $(date -r $end_time '+%H:%M:%S')"
    print_info "Target error rate: $(echo "scale=2; $failure_rate_per_10k / 100" | bc)%"
    echo ""
    
    while [ $(date +%s) -lt $end_time ]; do
        count=$((count + 1))
        
        # Use modulo 10000 for precise sub-1% rates
        local rand=$((RANDOM % 10000))
        if [ $rand -lt $failure_rate_per_10k ]; then
            log_network_response "503" "failure" "api.example.com" "/critical" "5000"
            failures=$((failures + 1))
        else
            log_network_response "200" "success" "api.example.com" "/critical" "45"
            successes=$((successes + 1))
        fi
        
        # Progress update every 60 seconds
        local elapsed=$(($(date +%s) - start_time))
        if [ $((count % 300)) -eq 0 ]; then
            local current_error_rate=$(echo "scale=3; $failures * 100 / $count" | bc)
            local remaining=$((duration_seconds - elapsed))
            print_info "[${elapsed}s] Sent: $count | Failures: $failures | Error rate: ${current_error_rate}% | Remaining: ${remaining}s"
        fi
        
        sleep $delay
    done
    
    local total_time=$(($(date +%s) - start_time))
    local final_error_rate=$(echo "scale=3; $failures * 100 / $count" | bc)
    
    echo ""
    print_header "Test Complete!"
    print_info "Duration: ${total_time}s | Total: $count | Failures: $failures"
    print_info "Final error rate: ${final_error_rate}%"
    echo ""
    print_warning "✅ Check your SLO alerts!"
    print_info "Timeline: logger-cli timeline"
}

# ============================================================================
# Helper function for burn rate tests
# ============================================================================
run_burn_rate_test() {
    local duration_seconds=$1
    local failure_rate=$2
    local delay="0.2"  # 5 RPS
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local count=0
    local failures=0
    local successes=0
    
    print_info "Starting at $(date '+%H:%M:%S'), will run until $(date -r $end_time '+%H:%M:%S')"
    print_info "Delay between requests: ${delay}s (~5 RPS)"
    echo ""
    
    while [ $(date +%s) -lt $end_time ]; do
        count=$((count + 1))
        
        local rand=$((RANDOM % 100))
        if [ $rand -lt $failure_rate ]; then
            log_network_response "503" "failure" "api.example.com" "/critical" "5000"
            failures=$((failures + 1))
        else
            log_network_response "200" "success" "api.example.com" "/critical" "45"
            successes=$((successes + 1))
        fi
        
        # Progress update every 30 seconds (~150 requests)
        local elapsed=$(($(date +%s) - start_time))
        if [ $((count % 150)) -eq 0 ]; then
            local current_error_rate=$(echo "scale=2; $failures * 100 / $count" | bc)
            local current_burn_rate=$(echo "scale=1; $current_error_rate * 10" | bc)
            print_info "[${elapsed}s] Sent: $count | Failures: $failures | Error rate: ${current_error_rate}% | Burn rate: ${current_burn_rate}x"
        fi
        
        sleep $delay
    done
    
    local total_time=$(($(date +%s) - start_time))
    local final_error_rate=$(echo "scale=2; $failures * 100 / $count" | bc)
    local final_burn_rate=$(echo "scale=1; $final_error_rate * 10" | bc)
    
    echo ""
    print_header "Test Complete!"
    print_info "Duration: ${total_time}s | Total: $count | Failures: $failures"
    print_info "Final error rate: ${final_error_rate}% | Burn rate: ${final_burn_rate}x"
    echo ""
    print_warning "✅ Check your SLO alerts!"
    print_info "Timeline: logger-cli timeline"
}

# ============================================================================
# TEST SCENARIO 3: Gradual degradation (tests if you have 6hr window configured)
# ============================================================================
scenario_gradual_degradation() {
    print_header "SCENARIO 3: Gradual Degradation"
    print_info "Simulating gradual increase in errors over time"
    print_info "Note: Your config uses 1hr/5min windows, but this tests sustained degradation"
    
    # Simulate 6 batches, each representing ~1 hour of traffic
    local batches=6
    local requests_per_batch=100
    
    for batch in $(seq 1 $batches); do
        # Error rate increases with each batch
        local error_rate=$((batch * 2))  # 2%, 4%, 6%, 8%, 10%, 12%
        local failures=$((requests_per_batch * error_rate / 100))
        
        print_info "Batch $batch/6: Error rate = ${error_rate}%"
        
        for i in $(seq 1 $requests_per_batch); do
            if [ $i -le $failures ]; then
                log_network_response "500" "failure" "api.example.com" "/search" "1000"
            else
                log_network_response "200" "success" "api.example.com" "/search" "100"
            fi
            
            sleep 0.02
        done
        
        # Simulate time passage between batches (in real tests, this would be ~1 hour)
        print_info "Batch $batch complete. Waiting before next batch..."
        sleep 1
    done
    
    print_info "Scenario 3 complete: Gradual degradation simulation finished"
}

# ============================================================================
# TEST SCENARIO 4: Sudden outage (extreme burn rate)
# ============================================================================
scenario_sudden_outage() {
    print_header "SCENARIO 4: Sudden Outage (Extreme Burn Rate)"
    print_info "Simulating sudden complete outage - 100% failure rate"
    print_warning "This SHOULD trigger ALL SLO alert thresholds immediately"
    
    local total=100
    
    for i in $(seq 1 $total); do
        # All failures
        log_network_response "503" "failure" "api.example.com" "/checkout" "30000"
        
        print_error "[$i/$total] Complete failure - Service DOWN"
        sleep 0.05
    done
    
    print_error "Scenario 4 complete: Total outage simulation - 100% failure rate"
}

# ============================================================================
# TEST SCENARIO 5: Recovery pattern
# ============================================================================
scenario_recovery() {
    print_header "SCENARIO 5: Recovery Pattern"
    print_info "Simulating recovery from outage - improving success rate"
    
    # 5 phases: 20% -> 50% -> 80% -> 95% -> 99.9% success
    local phases=(20 50 80 95 99)
    local requests_per_phase=50
    
    for success_rate in "${phases[@]}"; do
        print_info "Phase: ${success_rate}% success rate"
        local successes=$((requests_per_phase * success_rate / 100))
        
        for i in $(seq 1 $requests_per_phase); do
            if [ $i -le $successes ]; then
                log_network_response "200" "success" "api.example.com" "/health" "30"
            else
                log_network_response "500" "failure" "api.example.com" "/health" "100"
            fi
            
            sleep 0.02
        done
        
        print_info "Phase complete. Moving to next recovery phase..."
        sleep 0.5
    done
    
    print_info "Scenario 5 complete: Recovery simulation finished"
}

# ============================================================================
# TEST SCENARIO 6: App Start Time TTFI SLO test
# ============================================================================
scenario_app_start_ttfi() {
    print_header "SCENARIO 6: App Start TTFI SLO Test"
    print_info "Testing SLO: 99.9% of app starts should have TTFI < 5000ms"
    print_info "This tests the typical mobile app SLO use case"
    
    local total=200
    local slow_starts=10  # 5% slow starts to trigger alert
    
    for i in $(seq 1 $total); do
        span_id="ttfi-$(uuidgen | tr '[:upper:]' '[:lower:]')"
        
        if [ $i -le $slow_starts ]; then
            # Slow app start (> 5000ms TTFI)
            local ttfi_ms=$((5500 + RANDOM % 2000))  # 5500-7500ms
            log_rate_event "total" "app_start_ttfi" "ttfi_measurement"
            # Don't log success for slow starts
            
            $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
                --log-type lifecycle \
                --log-level warn \
                --field "_span_id" "$span_id" \
                --field "ttfi_ms" "$ttfi_ms" \
                --field "within_slo" "false" \
                "app_start_slow"
            
            print_warning "[$i/$total] Slow app start: ${ttfi_ms}ms TTFI"
        else
            # Fast app start (< 5000ms TTFI)
            local ttfi_ms=$((500 + RANDOM % 2000))  # 500-2500ms
            log_rate_event "total" "app_start_ttfi" "ttfi_measurement"
            log_rate_event "success" "app_start_ttfi" "ttfi_measurement"
            
            $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
                --log-type lifecycle \
                --log-level info \
                --field "_span_id" "$span_id" \
                --field "ttfi_ms" "$ttfi_ms" \
                --field "within_slo" "true" \
                "app_start_fast"
        fi
        
        if [ $((i % 50)) -eq 0 ]; then
            print_info "Progress: $i/$total app starts logged"
        fi
        
        sleep 0.03
    done
    
    print_info "Scenario 6 complete: $(($total - $slow_starts)) fast, $slow_starts slow app starts"
    print_info "Success rate: $(echo "scale=2; ($total - $slow_starts) * 100 / $total" | bc)%"
}

# ============================================================================
# ============================================================================
# TEST SCENARIO 7: Quick Network Response SLO Test (matches 5-min short window)
# ============================================================================
scenario_quick_slo_test() {
    print_header "SCENARIO 7: Quick Network Response SLO Test"
    print_info "Your SLO config: Long Window = 1 hour, Short Window = 5 minutes"
    print_info ""
    print_info "This test runs for 5+ minutes to ensure data in BOTH windows:"
    print_info "  - Short window (5 min): Will have full data after 5 minutes"
    print_info "  - Long window (1 hr): Will have partial data (enough to alert)"
    print_info ""
    print_info "Using Network Response fields:"
    print_info "  _status_code: HTTP status code (200, 500, etc.)"
    print_info "  _result: success | failure | canceled"
    print_info "  _host, _path, _duration_ms"
    echo ""
    
    # Configuration - runs 6 minutes to fully cover the 5-min short window
    local duration_seconds=360   # 6 minutes (to ensure full 5-min window coverage)
    local requests_per_second=5  # 5 RPS = 300 per minute
    local failure_rate=15        # 15% failure rate (way above 99.9% SLO threshold)
    
    print_warning "Configuration:"
    print_warning "  Duration: ${duration_seconds}s (6 minutes - covers 5-min short window)"
    print_warning "  Rate: ~${requests_per_second} requests/second"
    print_warning "  Failure rate: ${failure_rate}%"
    print_warning "  Expected failures: ~$((duration_seconds * requests_per_second * failure_rate / 100))"
    print_warning "  Expected total: ~$((duration_seconds * requests_per_second))"
    echo ""
    print_warning "Alert should fire once 5-min short window is fully covered (~5 min)"
    print_warning "Burn rate: 15% / 0.1% = 150x (threshold is 16.8x for 7-day SLO)"
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local count=0
    local failures=0
    local successes=0
    # Fixed delay: 0.2 seconds = 5 requests per second
    local delay="0.2"
    
    print_info "Starting at $(date '+%H:%M:%S'), will run until $(date -r $end_time '+%H:%M:%S')"
    print_info "Delay between requests: ${delay}s (~5 RPS)"
    
    while [ $(date +%s) -lt $end_time ]; do
        count=$((count + 1))
        
        # Determine success or failure
        local rand=$((RANDOM % 100))
        if [ $rand -lt $failure_rate ]; then
            # Failure - use 5xx status codes
            local status_codes=(500 502 503 504)
            local status=${status_codes[$((RANDOM % 4))]}
            log_network_response "$status" "failure" "api.example.com" "/api/v1/data" "5000"
            failures=$((failures + 1))
        else
            # Success
            local latency=$((20 + RANDOM % 200))
            log_network_response "200" "success" "api.example.com" "/api/v1/data" "$latency"
            successes=$((successes + 1))
        fi
        
        # Progress update every 30 seconds
        local elapsed=$(($(date +%s) - start_time))
        if [ $((count % 150)) -eq 0 ]; then
            local actual_rate=$(echo "scale=1; $count / $elapsed" | bc)
            local current_error_rate=$(echo "scale=1; $failures * 100 / $count" | bc)
            print_info "[${elapsed}s] Sent: $count | Successes: $successes | Failures: $failures | Error rate: ${current_error_rate}% | Rate: ${actual_rate}/s"
        fi
        
        sleep $delay
    done
    
    local total_time=$(($(date +%s) - start_time))
    local final_error_rate=$(echo "scale=2; $failures * 100 / $count" | bc)
    
    echo ""
    print_header "Test Complete"
    print_info "Duration: ${total_time} seconds"
    print_info "Total requests: $count"
    print_info "Successes: $successes"
    print_info "Failures: $failures"
    print_info "Error rate: ${final_error_rate}%"
    print_info "Effective RPS: $(echo "scale=1; $count / $total_time" | bc)"
    echo ""
    print_warning "Check your SLO alerts - they should fire within 1-5 minutes"
    print_info "Timeline URL: https://timeline.bitdrift.dev"
}

# ============================================================================
# TEST SCENARIO: Reset/Resolve Alerts (100% success traffic)
# ============================================================================
# This scenario sends 100% healthy traffic to auto-resolve any firing alerts.
# The alert cron needs to see error rate below threshold in BOTH windows.
#
# For the fastest resolution (5-min short window + 1-hr long window):
# - Need 5+ minutes of healthy traffic to clear the short window
# - May need longer if long window still has high error rate
# - Alert cron runs every 5 minutes with 2-minute aggregation delay
#
# Recommended: Run for 10-15 minutes to ensure clean slate
# ============================================================================
scenario_reset_alerts() {
    print_header "SCENARIO: Reset/Resolve Alerts (100% healthy traffic)"
    
    local duration_minutes="${1:-10}"  # Default 10 minutes
    local duration_seconds=$((duration_minutes * 60))
    local requests_per_second=5
    
    print_info "Purpose: Send 100% healthy traffic to auto-resolve any firing alerts"
    print_info "Duration: ${duration_minutes} minutes (${duration_seconds} seconds)"
    print_info "Rate: ~${requests_per_second} requests/second"
    print_info "Expected requests: ~$((duration_seconds * requests_per_second))"
    echo ""
    print_warning "WHY THIS IS NEEDED:"
    print_warning "  - Alerts stay in 'firing' state until error rate drops below threshold"
    print_warning "  - While alert is firing, NO new notifications are sent"
    print_warning "  - Alert cron checks every 5 minutes (with 2-min delay)"
    print_warning "  - Both SHORT and LONG windows must show healthy rates to resolve"
    echo ""
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local count=0
    local delay="0.2"  # 5 RPS
    
    print_info "Starting at $(date '+%H:%M:%S'), will run until $(date -r $end_time '+%H:%M:%S')"
    
    while [ $(date +%s) -lt $end_time ]; do
        count=$((count + 1))
        
        # 100% success - random latency between 20-200ms
        local latency=$((20 + RANDOM % 180))
        log_network_response "200" "success" "api.example.com" "/api/v1/data" "$latency"
        
        # Progress update every 30 seconds
        local elapsed=$(($(date +%s) - start_time))
        if [ $((count % 150)) -eq 0 ]; then
            local remaining=$((duration_seconds - elapsed))
            local actual_rate=$(echo "scale=1; $count / $elapsed" | bc)
            print_info "[${elapsed}s] Sent: $count | 100% success | Rate: ${actual_rate}/s | Remaining: ${remaining}s"
        fi
        
        sleep $delay
    done
    
    local total_time=$(($(date +%s) - start_time))
    
    echo ""
    print_header "Reset Complete"
    print_info "Duration: ${total_time} seconds"
    print_info "Total requests: $count (100% success)"
    print_info "Effective RPS: $(echo "scale=1; $count / $total_time" | bc)"
    echo ""
    print_warning "Alert should auto-resolve within 5-10 minutes"
    print_warning "Wait for alert cron to run (every 5 min) before re-testing"
    print_info "Check workflow status at: https://timeline.bitdrift.dev"
}

# ============================================================================
# Interactive Menu
# ============================================================================
show_menu() {
    print_header "SLO Alert Testing Menu"
    echo ""
    echo "  RESET & UTILITIES:"
    echo "    0) Reset/Resolve Alerts (100% success for 10 min)"
    echo ""
    echo "  TARGETED THRESHOLD TESTS (7-day SLO defaults):"
    echo "    T1) Threshold #1 (1hr/5min)   - 10% error, 6 min → fires ALL"
    echo "    T2) Threshold #2 (6hr/30min)  - 2% error, 35 min → fires #2, #3"
    echo "    T3) Threshold #3 (24hr/2hr)   - 0.4% error, 2.5 hr → fires ONLY #3"
    echo ""
    echo "  GENERAL SCENARIOS:"
    echo "    1) Healthy Baseline (99.9% success - should NOT alert)"
    echo "    2) High Burn Rate - ALL thresholds (5% error)"
    echo "    3) Gradual Degradation"
    echo "    4) Sudden Outage (100% failure)"
    echo "    5) Recovery Pattern"
    echo "    6) App Start TTFI SLO Test"
    echo "    7) Quick Network Response SLO Test"
    echo ""
    echo "    q) Quit"
    echo ""
    echo "  THRESHOLD HIERARCHY (7-day SLO, 99.9% target):"
    echo "    #1: >1.68% fires all  |  #2: >0.56% fires #2,#3  |  #3: >0.28% fires only #3"
    echo ""
}

# ============================================================================
# Interactive Menu
# ============================================================================
main() {
    print_header "bitdrift SLO Alert Testing Utility"
    
    check_server
    
    if [ $# -eq 1 ]; then
        case "$1" in
            0) scenario_reset_alerts ;;
            1) scenario_healthy_baseline ;;
            2) scenario_high_burn_rate_1hr ;;
            t1|T1) scenario_trigger_threshold_1 ;;
            t2|T2) scenario_trigger_threshold_2 ;;
            t3|T3) scenario_trigger_threshold_3 ;;
            t4|T4) scenario_trigger_threshold_4 ;;
            3) scenario_gradual_degradation ;;
            4) scenario_sudden_outage ;;
            5) scenario_recovery ;;
            6) scenario_app_start_ttfi ;;
            7) scenario_quick_slo_test ;;
            *)
                echo "Unknown scenario: $1"
                echo "Usage: $0 [0|1|2|T1|T2|T3|T4|3|4|5|6|7]"
                exit 1
                ;;
        esac
    else
        while true; do
            show_menu
            read -p "Select scenario: " choice
            case "$choice" in
                0) scenario_reset_alerts ;;
                1) scenario_healthy_baseline ;;
                2) scenario_high_burn_rate_1hr ;;
                t1|T1) scenario_trigger_threshold_1 ;;
                t2|T2) scenario_trigger_threshold_2 ;;
                t3|T3) scenario_trigger_threshold_3 ;;
                t4|T4) scenario_trigger_threshold_4 ;;
                3) scenario_gradual_degradation ;;
                4) scenario_sudden_outage ;;
                5) scenario_recovery ;;
                6) scenario_app_start_ttfi ;;
                7) scenario_quick_slo_test ;;
                q|Q) 
                    print_info "Goodbye!"
                    exit 0
                    ;;
                *)
                    print_error "Invalid choice."
                    ;;
            esac
            echo ""
            read -p "Press Enter to continue..."
        done
    fi
}

main "$@"

