#!/bin/bash

# =============================================================================
# Navigation & Trigger Testing Script for Timeline
# =============================================================================
#
# This script generates a long session with diverse logs to test navigation
# issues with triggers (< > arrows) in the timeline view.
#
# Based on issue observations:
# - Navigation arrows skip trigger logs unexpectedly
# - Timeline bar "flashes" and "skips back" when dragging
# - Log lines disappear when scrolling at the end
# - Triggers should auto-expand log details when navigated to
#
# This script creates a session with:
# - Multiple trigger-matching logs (lifecycle, crashes, UX events)
# - Regular logs interspersed between trigger logs
# - Spans (network requests) with start/end pairs
# - Various log levels and types
# - Enough logs to fill several pages
#
# =============================================================================

set -e

# Configuration
LOGGER_HOST="${LOGGER_HOST:-localhost}"
LOGGER_PORT="${LOGGER_PORT:-5501}"
LOGGER_CLI="${LOGGER_CLI:-logger-cli}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Counters
TOTAL_LOGS=0
TRIGGER_LOGS=0

print_header() {
    echo ""
    echo -e "${BLUE}=================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=================================================================${NC}"
}

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_trigger() {
    echo -e "${MAGENTA}[TRIGGER]${NC} $1"
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
        echo "  API_KEY=<your-key> cargo run -p logger-cli -- start"
        echo ""
        exit 1
    fi
    print_info "Logger server is running on $LOGGER_HOST:$LOGGER_PORT"
}

# Generate a UUID for span IDs
generate_uuid() {
    cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen | tr '[:upper:]' '[:lower:]'
}

# ============================================================================
# LOG FUNCTIONS - Trigger-Matching Logs (will match workflow triggers)
# ============================================================================

# Lifecycle: SDKConfigured (TRIGGER)
log_sdk_configured() {
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type lifecycle \
        --log-level info \
        --field "_sdk_version" "0.22.3" \
        --field "_duration_ms" "36.289" \
        --field "_session_strategy" "fixed" \
        "SDKConfigured"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "SDKConfigured - Lifecycle trigger"
}

# Lifecycle: AppLaunchTTI (TRIGGER)
log_app_launch_tti() {
    local duration_ms="$1"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type lifecycle \
        --log-level info \
        --field "_duration_ms" "$duration_ms" \
        "AppLaunchTTI"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "AppLaunchTTI - ${duration_ms}ms"
}

# Lifecycle: AppCreate (TRIGGER)
log_app_create() {
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type lifecycle \
        --log-level info \
        "AppCreate"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "AppCreate - Lifecycle"
}

# Lifecycle: AppStart (TRIGGER)
log_app_start() {
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type lifecycle \
        --log-level info \
        "AppStart"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "AppStart - Lifecycle"
}

# Lifecycle: AppResume (TRIGGER)
log_app_resume() {
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type lifecycle \
        --log-level info \
        "AppResume"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "AppResume - Lifecycle"
}

# Lifecycle: AppPause (TRIGGER)
log_app_pause() {
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type lifecycle \
        --log-level info \
        "AppPause"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "AppPause - Lifecycle"
}

# Lifecycle: AppExit - Non-Fatal (TRIGGER)
log_app_exit_non_fatal() {
    local error_type="$1"
    local error_details="$2"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type lifecycle \
        --log-level error \
        --field "_app_exit_reason" "Strict Mode Violation" \
        --field "_app_exit_info" "$error_type" \
        --field "_app_exit_details" "$error_details" \
        --field "_fatal_issue_mechanism" "BUILT_IN" \
        --field "_crash_artifact_id" "$(generate_uuid)" \
        "AppExit"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "AppExit Non-Fatal - $error_type"
}

# Lifecycle: AppExit - Fatal (TRIGGER)
log_app_exit_fatal() {
    local error_type="$1"
    local error_details="$2"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type lifecycle \
        --log-level error \
        --field "_app_exit_reason" "Crash" \
        --field "_app_exit_info" "$error_type" \
        --field "_app_exit_details" "$error_details" \
        --field "_fatal_issue_mechanism" "BUILT_IN" \
        --field "_crash_artifact_id" "$(generate_uuid)" \
        "AppExit"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "AppExit Fatal - $error_type"
}

# UX: ScreenView (TRIGGER)
log_screen_view() {
    local screen_name="$1"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type ux \
        --log-level info \
        --field "_screen_name" "$screen_name" \
        "ScreenView"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "ScreenView - $screen_name"
}

# UX: DroppedFrame (TRIGGER)
log_dropped_frame() {
    local screen_name="$1"
    local duration_ms="$2"
    local issue_type="$3"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type ux \
        --log-level warn \
        --field "_screen_name" "$screen_name" \
        --field "_duration_ms" "$duration_ms" \
        --field "_frame_issue_type" "$issue_type" \
        "DroppedFrame"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "DroppedFrame - $screen_name ($issue_type ${duration_ms}ms)"
}

# Device: ThermalStateChange (TRIGGER)
log_thermal_state() {
    local state="$1"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type device \
        --log-level info \
        --field "_thermal_state" "$state" \
        "ThermalStateChange"
    : $((++TOTAL_LOGS))
    : $((++TRIGGER_LOGS))
    print_trigger "ThermalStateChange - $state"
}

# ============================================================================
# LOG FUNCTIONS - Span Logs (HTTP requests)
# ============================================================================

# HTTP Request Start
log_http_request_start() {
    local span_id="$1"
    local method="$2"
    local host="$3"
    local path="$4"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type span \
        --log-level debug \
        --field "_span_id" "$span_id" \
        --field "_span_type" "start" \
        --field "_span_name" "_http" \
        --field "_method" "$method" \
        --field "_host" "$host" \
        --field "_path" "$path" \
        --field "_path_template" "$path" \
        --field "_request_body_bytes_expected_to_send_count" "52" \
        "HTTPRequest"
    : $((++TOTAL_LOGS))
    print_info "→ HTTP $method $path"
}

# HTTP Response End
log_http_response() {
    local span_id="$1"
    local method="$2"
    local host="$3"
    local path="$4"
    local status_code="$5"
    local duration_ms="$6"
    local result="$7"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type span \
        --log-level debug \
        --field "_span_id" "$span_id" \
        --field "_span_type" "end" \
        --field "_span_name" "_http" \
        --field "_method" "$method" \
        --field "_host" "$host" \
        --field "_path" "$path" \
        --field "_path_template" "$path" \
        --field "_status_code" "$status_code" \
        --field "_duration_ms" "$duration_ms" \
        --field "_result" "$result" \
        --field "_protocol" "h2" \
        --field "_response_latency_ms" "$duration_ms" \
        --field "_request_body_bytes_sent_count" "52" \
        --field "_response_body_bytes_received_count" "1024" \
        "HTTPResponse"
    : $((++TOTAL_LOGS))
    print_info "← HTTP $method $path - $status_code ($result, ${duration_ms}ms)"
}

# Complete HTTP request (start + end with simulated latency)
log_http_request_complete() {
    local method="$1"
    local host="$2"
    local path="$3"
    local status_code="$4"
    local duration_ms="$5"
    local result="$6"
    local span_id=$(generate_uuid)
    
    log_http_request_start "$span_id" "$method" "$host" "$path"
    # Small delay to simulate network latency
    sleep 0.05
    log_http_response "$span_id" "$method" "$host" "$path" "$status_code" "$duration_ms" "$result"
}

# ============================================================================
# LOG FUNCTIONS - Regular Logs (not trigger-matching)
# ============================================================================

# Regular info log
log_info() {
    local message="$1"
    local source="${2:-Application}"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type normal \
        --log-level info \
        --field "source" "$source" \
        "$message"
    : $((++TOTAL_LOGS))
}

# Regular debug log
log_debug() {
    local message="$1"
    local source="${2:-Application}"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type normal \
        --log-level debug \
        --field "source" "$source" \
        "$message"
    : $((++TOTAL_LOGS))
}

# Regular warning log
log_warn() {
    local message="$1"
    local source="${2:-Application}"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type normal \
        --log-level warn \
        --field "source" "$source" \
        "$message"
    : $((++TOTAL_LOGS))
}

# Regular error log
log_error() {
    local message="$1"
    local source="${2:-Application}"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type normal \
        --log-level error \
        --field "source" "$source" \
        "$message"
    : $((++TOTAL_LOGS))
}

# Timber log (like in sample)
log_timber() {
    local message="$1"
    $LOGGER_CLI --host "$LOGGER_HOST" --port "$LOGGER_PORT" log \
        --log-type normal \
        --log-level info \
        --field "source" "Timber" \
        "$message"
    : $((++TOTAL_LOGS))
}

# ============================================================================
# SCENARIO: App Initialization Sequence
# ============================================================================
scenario_app_init() {
    print_header "App Initialization Sequence"
    
    log_sdk_configured
    sleep 0.1
    
    log_timber "Bitdrift Logger initialized with session_url=https://timeline.bitdrift.dev/s/test-session?utm_source=sdk"
    sleep 0.05
    
    log_thermal_state "NONE"
    sleep 0.05
    
    log_screen_view "first_fragment"
    sleep 0.05
    
    # Non-fatal error during init
    log_app_exit_non_fatal \
        "android.os.strictmode.UntaggedSocketViolation" \
        "Untagged socket detected; use TrafficStats.setTrafficStatsTag() to track all network usage"
    sleep 0.1
    
    log_timber "PapaEvent.AppLaunch"
    sleep 0.05
    
    log_app_launch_tti "979"
    log_app_create
    log_app_start
    log_app_resume
    sleep 0.1
    
    log_screen_view "home_tab"
}

# ============================================================================
# SCENARIO: Network Activity
# ============================================================================
scenario_network_activity() {
    local count="${1:-20}"
    print_header "Network Activity ($count requests)"
    
    local endpoints=(
        "/v1/device/code"
        "/v1/user/profile"
        "/v1/feed/timeline"
        "/v1/notifications"
        "/v1/search"
        "/v1/analytics/events"
        "/v1/config"
        "/v1/auth/refresh"
        "/v2/messages"
        "/v2/sync"
    )
    
    local methods=("GET" "POST" "PUT" "PATCH")
    
    for i in $(seq 1 $count); do
        local endpoint_idx=$((RANDOM % ${#endpoints[@]}))
        local method_idx=$((RANDOM % ${#methods[@]}))
        local endpoint="${endpoints[$endpoint_idx]}"
        local method="${methods[$method_idx]}"
        
        # 90% success, 10% failure
        if [ $((RANDOM % 10)) -lt 9 ]; then
            local status_code="200"
            local result="success"
        else
            local status_code="500"
            local result="failure"
        fi
        
        local duration=$((50 + RANDOM % 500))
        
        log_http_request_complete "$method" "api.bitdrift.dev" "$endpoint" "$status_code" "$duration" "$result"
        
        # Add some regular logs between requests
        if [ $((i % 5)) -eq 0 ]; then
            log_debug "Processing request batch $i" "NetworkManager"
        fi
        
        sleep 0.02
    done
}

# ============================================================================
# SCENARIO: User Navigation
# ============================================================================
scenario_user_navigation() {
    local count="${1:-10}"
    print_header "User Navigation ($count screens)"
    
    local screens=(
        "home_tab"
        "navigate_tab"
        "profile_tab"
        "settings_screen"
        "web_view_fragment"
        "detail_view"
        "search_results"
        "notifications_list"
        "chat_screen"
        "media_gallery"
        "checkout_flow"
        "order_history"
        "help_center"
        "about_page"
        "preferences_screen"
    )
    
    for i in $(seq 1 $count); do
        local screen_idx=$((RANDOM % ${#screens[@]}))
        local screen="${screens[$screen_idx]}"
        
        log_screen_view "$screen"
        
        # Occasionally add dropped frames (performance issues)
        if [ $((RANDOM % 5)) -eq 0 ]; then
            local duration=$((50 + RANDOM % 200))
            local issue_types=("Slow" "Frozen")
            local issue_type_idx=$((RANDOM % 2))
            log_dropped_frame "$screen" "$duration" "${issue_types[$issue_type_idx]}"
        fi
        
        # Add some user activity logs
        log_info "User interacted with $screen" "UserActivity"
        
        if [ $((i % 3)) -eq 0 ]; then
            log_debug "View hierarchy updated for $screen" "UIFramework"
        fi
        
        sleep 0.1
    done
}

# ============================================================================
# SCENARIO: App Lifecycle Events
# ============================================================================
scenario_lifecycle_events() {
    local count="${1:-5}"
    print_header "Lifecycle Events ($count cycles)"
    
    for i in $(seq 1 $count); do
        print_info "Lifecycle cycle $i"
        
        # App goes to background
        log_app_pause
        log_info "App entered background" "LifecycleManager"
        sleep 0.1
        
        # Some background activity
        log_debug "Background task executing" "BackgroundService"
        log_http_request_complete "POST" "api.bitdrift.dev" "/v1/analytics/events" "200" "150" "success"
        sleep 0.1
        
        # App returns to foreground
        log_app_resume
        log_info "App returned to foreground" "LifecycleManager"
        sleep 0.1
        
        # Potentially trigger thermal events
        if [ $((RANDOM % 3)) -eq 0 ]; then
            local states=("LIGHT" "MODERATE" "SEVERE")
            local state_idx=$((RANDOM % 3))
            log_thermal_state "${states[$state_idx]}"
            sleep 0.05
            log_thermal_state "NONE"
        fi
        
        sleep 0.1
    done
}

# ============================================================================
# SCENARIO: Errors and Crashes
# ============================================================================
scenario_errors_and_crashes() {
    local count="${1:-5}"
    print_header "Errors and Crashes ($count events)"
    
    local error_types=(
        "java.lang.NullPointerException"
        "java.lang.IllegalStateException"
        "java.lang.IndexOutOfBoundsException"
        "android.view.InflateException"
        "java.lang.OutOfMemoryError"
        "java.net.SocketTimeoutException"
        "java.io.IOException"
        "java.lang.SecurityException"
        "java.lang.ClassNotFoundException"
        "java.lang.NoSuchMethodError"
    )
    
    local error_details=(
        "Attempt to invoke virtual method on a null object reference"
        "Cannot perform this action after onSaveInstanceState"
        "Index 5 out of bounds for length 3"
        "Binary XML file line #24: Error inflating class android.widget.Button"
        "Failed to allocate a 12840012 byte allocation with 4194304 free bytes"
        "Connection timed out"
        "No space left on device"
        "Permission denied"
        "Class not found using the boot class loader"
        "No static method instrument(Landroid/webkit/WebView;)V in class"
    )
    
    for i in $(seq 1 $count); do
        local idx=$((RANDOM % ${#error_types[@]}))
        local error_type="${error_types[$idx]}"
        local error_detail="${error_details[$idx]}"
        
        # Add some context logs before the error
        log_warn "Potential issue detected in component $i" "ErrorHandler"
        log_debug "Stack trace captured" "CrashReporter"
        
        # 30% fatal, 70% non-fatal
        if [ $((RANDOM % 10)) -lt 3 ]; then
            log_app_exit_fatal "$error_type" "$error_detail"
        else
            log_app_exit_non_fatal "$error_type" "$error_detail"
        fi
        
        sleep 0.2
    done
}

# ============================================================================
# SCENARIO: Heavy Logging (fills pages)
# ============================================================================
scenario_heavy_logging() {
    local count="${1:-100}"
    print_header "Heavy Logging ($count logs)"
    
    local sources=("Database" "NetworkManager" "UIFramework" "CacheManager" "Analytics" "Preferences" "Storage" "Auth")
    
    for i in $(seq 1 $count); do
        local source_idx=$((RANDOM % ${#sources[@]}))
        local source="${sources[$source_idx]}"
        local level=$((RANDOM % 4))
        
        case $level in
            0) log_debug "Debug message $i: Processing data from $source" "$source" ;;
            1) log_info "Info message $i: Operation completed in $source" "$source" ;;
            2) log_warn "Warning message $i: Degraded performance in $source" "$source" ;;
            3) log_error "Error message $i: Operation failed in $source" "$source" ;;
        esac
        
        # Progress indicator
        if [ $((i % 25)) -eq 0 ]; then
            print_info "Progress: $i/$count heavy logs"
        fi
        
        sleep 0.01
    done
}

# ============================================================================
# SCENARIO: Mixed Activity (realistic session)
# ============================================================================
scenario_mixed_activity() {
    local duration_seconds="${1:-60}"
    print_header "Mixed Activity (${duration_seconds}s realistic session)"
    
    local start_time=$(date +%s)
    local end_time=$((start_time + duration_seconds))
    local iteration=0
    
    while [ $(date +%s) -lt $end_time ]; do
        iteration=$((iteration + 1))
        
        # Random activity type
        local activity=$((RANDOM % 10))
        
        case $activity in
            0|1|2)
                # Network request (30%)
                local endpoints=("/v1/data" "/v1/sync" "/v2/feed" "/v1/user")
                local endpoint_idx=$((RANDOM % ${#endpoints[@]}))
                log_http_request_complete "GET" "api.bitdrift.dev" "${endpoints[$endpoint_idx]}" "200" "$((50 + RANDOM % 300))" "success"
                ;;
            3|4)
                # Screen navigation (20%)
                local screens=("home" "detail" "list" "settings" "profile")
                local screen_idx=$((RANDOM % ${#screens[@]}))
                log_screen_view "${screens[$screen_idx]}_screen"
                ;;
            5)
                # Dropped frame (10%)
                log_dropped_frame "current_screen" "$((60 + RANDOM % 100))" "Slow"
                ;;
            6)
                # Error (10%)
                log_error "Transient error occurred" "ErrorHandler"
                ;;
            7|8|9)
                # Regular log (30%)
                local levels=("debug" "info" "warn")
                local level_idx=$((RANDOM % 3))
                case ${levels[$level_idx]} in
                    "debug") log_debug "Activity iteration $iteration" "Application" ;;
                    "info") log_info "Processing iteration $iteration" "Application" ;;
                    "warn") log_warn "Slow operation in iteration $iteration" "Application" ;;
                esac
                ;;
        esac
        
        # Every 10 iterations, add a lifecycle event
        if [ $((iteration % 10)) -eq 0 ]; then
            if [ $((RANDOM % 2)) -eq 0 ]; then
                log_app_pause
                sleep 0.2
                log_app_resume
            fi
        fi
        
        sleep 0.05
    done
    
    print_info "Mixed activity completed: $iteration iterations"
}

# ============================================================================
# SCENARIO: Sparse Triggers (1 trigger every ~25 logs - matches iCabbi session)
# ============================================================================
scenario_sparse_triggers() {
    local total_logs="${1:-500}"
    local trigger_interval="${2:-25}"
    print_header "Sparse Triggers ($total_logs logs, 1 trigger every ~$trigger_interval logs)"
    
    # IMPORTANT: Send SDKConfigured first to initialize the session for workflows
    print_info "Sending SDKConfigured to initialize session..."
    log_sdk_configured
    
    local sources=("Database" "NetworkManager" "UIFramework" "CacheManager" "Analytics" "Preferences" "Storage" "Auth" "Sync" "Location")
    local screens=("home_tab" "navigate_tab" "profile_tab" "settings_screen" "detail_view" "search_results" "notifications_list" "chat_screen")
    local endpoints=("/v1/data" "/v1/sync" "/v2/feed" "/v1/user" "/v1/config" "/v1/notifications" "/v2/messages")
    
    local log_count=0
    local trigger_count=0
    local next_trigger=$((RANDOM % 10 + trigger_interval - 5))  # First trigger between 20-30
    
    # Exception types for app-crash triggers
    local exceptions=("java.lang.NullPointerException" "java.lang.IllegalStateException" "java.lang.OutOfMemoryError" "java.lang.RuntimeException" "java.lang.IndexOutOfBoundsException")
    local exception_details=("Attempt to invoke virtual method on null object" "Fragment not attached to Activity" "Failed to allocate memory" "Unexpected error in processing" "Index 5 out of bounds for length 3")
    
    while [ $log_count -lt $total_logs ]; do
        log_count=$((log_count + 1))
        
        # Check if it's time for a trigger log
        if [ $log_count -ge $next_trigger ]; then
            # Generate an app-crash log that matches the OOTB 'app-crash' matcher
            # This requires: log_type=lifecycle, message=AppExit, _app_exit_reason=Crash
            local exc_idx=$((RANDOM % ${#exceptions[@]}))
            log_app_exit_fatal "${exceptions[$exc_idx]}" "${exception_details[$exc_idx]}"
            trigger_count=$((trigger_count + 1))
            # Schedule next trigger (randomize interval a bit: 20-30 logs apart)
            next_trigger=$((log_count + trigger_interval - 5 + RANDOM % 10))
        else
            # Generate a regular (non-trigger) log - these WON'T match the workflow
            local log_type=$((RANDOM % 10))
            case $log_type in
                0|1|2)
                    # Network span (30%) - won't trigger unless workflow matches network-response
                    local endpoint_idx=$((RANDOM % ${#endpoints[@]}))
                    local method_idx=$((RANDOM % 3))
                    local methods=("GET" "POST" "PUT")
                    log_http_request_complete "${methods[$method_idx]}" "api.bitdrift.dev" "${endpoints[$endpoint_idx]}" "200" "$((50 + RANDOM % 400))" "success"
                    # This adds 2 logs (start+end), so increment extra
                    log_count=$((log_count + 1))
                    ;;
                3|4)
                    # Debug log (20%) - won't trigger
                    local source_idx=$((RANDOM % ${#sources[@]}))
                    log_debug "Processing data batch $log_count" "${sources[$source_idx]}"
                    ;;
                5|6)
                    # Info log (20%) - won't trigger
                    local source_idx=$((RANDOM % ${#sources[@]}))
                    log_info "Operation completed at step $log_count" "${sources[$source_idx]}"
                    ;;
                7|8)
                    # Timber/verbose log (20%) - won't trigger
                    log_timber "Verbose log entry $log_count - internal state update"
                    ;;
                9)
                    # Warning (10%) - won't trigger
                    local source_idx=$((RANDOM % ${#sources[@]}))
                    log_warn "Slow operation detected at $log_count" "${sources[$source_idx]}"
                    ;;
            esac
        fi
        
        # Progress indicator every 50 logs
        if [ $((log_count % 50)) -eq 0 ]; then
            print_info "Progress: $log_count/$total_logs logs ($trigger_count triggers so far)"
        fi
        
        sleep 0.01
    done
    
    print_info "Sparse triggers completed: $log_count total logs, $trigger_count triggers"
    if [ $trigger_count -gt 0 ]; then
        print_info "Actual trigger ratio: 1 trigger per $((log_count / trigger_count)) logs"
    fi
    echo ""
    echo -e "${YELLOW}NOTE: Trigger logs are 'AppExit' with _app_exit_reason='Crash'${NC}"
    echo -e "${YELLOW}      These match the 'app-crash' OOTB rule (Fatal Issue - JVM).${NC}"
    echo -e "${YELLOW}      Make sure your workflow uses this OOTB event with Android platform.${NC}"
}

# ============================================================================
# MAIN: Full Test Session
# ============================================================================
full_test_session() {
    print_header "FULL TEST SESSION FOR NAVIGATION DEBUGGING"
    echo ""
    echo -e "${CYAN}This will generate a comprehensive session with:${NC}"
    echo -e "${CYAN}  - Sparse trigger-matching logs (~1 every 25 logs)${NC}"
    echo -e "${CYAN}  - Many regular logs interspersed between triggers${NC}"
    echo -e "${CYAN}  - Network spans (HTTP request start/end pairs)${NC}"
    echo -e "${CYAN}  - Various log levels and types${NC}"
    echo -e "${CYAN}  - Enough logs to fill several pages${NC}"
    echo ""
    
    check_server
    
    # Reset counters
    TOTAL_LOGS=0
    TRIGGER_LOGS=0
    
    echo ""
    print_info "Starting log generation..."
    echo ""
    
    # 1. App Initialization (a few triggers at the start)
    scenario_app_init
    
    # 2. Main bulk: sparse triggers matching the iCabbi pattern (1 trigger per ~25 logs)
    scenario_sparse_triggers 600 25
    
    # 3. Final lifecycle
    log_app_pause
    sleep 0.1
    log_app_resume
    
    # Summary
    print_header "SESSION GENERATION COMPLETE"
    echo ""
    echo -e "${GREEN}Total logs generated: $TOTAL_LOGS${NC}"
    echo -e "${MAGENTA}Trigger-matching logs: $TRIGGER_LOGS${NC}"
    echo -e "${CYAN}Ratio: ~1 trigger per $((TOTAL_LOGS / (TRIGGER_LOGS > 0 ? TRIGGER_LOGS : 1))) logs${NC}"
    echo ""
    echo -e "${CYAN}To view the session:${NC}"
    echo -e "${CYAN}  $LOGGER_CLI --host $LOGGER_HOST --port $LOGGER_PORT timeline${NC}"
    echo ""
}

# ============================================================================
# MAIN: Original Full Test Session (many triggers)
# ============================================================================
full_test_session_original() {
    print_header "ORIGINAL FULL TEST SESSION (many triggers)"
    echo ""
    echo -e "${CYAN}This will generate a session with many triggers clustered together.${NC}"
    echo ""
    
    check_server
    
    # Reset counters
    TOTAL_LOGS=0
    TRIGGER_LOGS=0
    
    echo ""
    print_info "Starting log generation..."
    echo ""
    
    # 1. App Initialization
    scenario_app_init
    
    # 2. Initial network activity
    scenario_network_activity 30
    
    # 3. User navigation
    scenario_user_navigation 15
    
    # 4. More network activity
    scenario_network_activity 20
    
    # 5. Lifecycle events
    scenario_lifecycle_events 5
    
    # 6. Heavy logging to fill pages
    scenario_heavy_logging 150
    
    # 7. Errors and crashes
    scenario_errors_and_crashes 8
    
    # 8. More navigation
    scenario_user_navigation 10
    
    # 9. More heavy logging
    scenario_heavy_logging 100
    
    # 10. Final mixed activity
    scenario_mixed_activity 30
    
    # 11. Final lifecycle
    log_app_pause
    sleep 0.1
    log_app_resume
    
    # Summary
    print_header "SESSION GENERATION COMPLETE"
    echo ""
    echo -e "${GREEN}Total logs generated: $TOTAL_LOGS${NC}"
    echo -e "${MAGENTA}Trigger-matching logs: $TRIGGER_LOGS${NC}"
    echo ""
    echo -e "${CYAN}To view the session:${NC}"
    echo -e "${CYAN}  $LOGGER_CLI --host $LOGGER_HOST --port $LOGGER_PORT timeline${NC}"
    echo ""
}

# ============================================================================
# Usage
# ============================================================================
usage() {
    echo "Navigation Testing Script for Timeline"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  full            - Run full test session with sparse triggers (default)"
    echo "  full-original   - Run original test session (many triggers)"
    echo "  sparse [n] [i]  - Sparse triggers (n logs, 1 trigger every i logs; default 500, 25)"
    echo "  init            - App initialization sequence only"
    echo "  network [n]     - Network activity (n requests, default 20)"
    echo "  nav [n]         - User navigation (n screens, default 10)"
    echo "  lifecycle [n]   - Lifecycle events (n cycles, default 5)"
    echo "  errors [n]      - Errors and crashes (n events, default 5)"
    echo "  heavy [n]       - Heavy logging (n logs, default 100)"
    echo "  mixed [s]       - Mixed activity (s seconds, default 60)"
    echo ""
    echo "Environment variables:"
    echo "  LOGGER_HOST   - Logger server host (default: localhost)"
    echo "  LOGGER_PORT   - Logger server port (default: 5501)"
    echo "  LOGGER_CLI    - Path to logger-cli binary (default: logger-cli)"
    echo ""
    echo "Example:"
    echo "  # Start the logger server first:"
    echo "  API_KEY=<your-key> cargo run -p logger-cli -- start"
    echo ""
    echo "  # Then run this script:"
    echo "  $0 full"
    echo ""
    echo "  # Or generate 1000 logs with 1 trigger every 30 logs:"
    echo "  $0 sparse 1000 30"
}

# ============================================================================
# Main
# ============================================================================
case "${1:-full}" in
    full)
        full_test_session
        ;;
    full-original)
        full_test_session_original
        ;;
    sparse)
        check_server
        scenario_sparse_triggers "${2:-500}" "${3:-25}"
        ;;
    init)
        check_server
        scenario_app_init
        ;;
    network)
        check_server
        scenario_network_activity "${2:-20}"
        ;;
    nav)
        check_server
        scenario_user_navigation "${2:-10}"
        ;;
    lifecycle)
        check_server
        scenario_lifecycle_events "${2:-5}"
        ;;
    errors)
        check_server
        scenario_errors_and_crashes "${2:-5}"
        ;;
    heavy)
        check_server
        scenario_heavy_logging "${2:-100}"
        ;;
    mixed)
        check_server
        scenario_mixed_activity "${2:-60}"
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        echo "Unknown command: $1"
        usage
        exit 1
        ;;
esac
