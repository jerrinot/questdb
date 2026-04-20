/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.http2;

/**
 * Stateless flow-control window arithmetic per RFC 7540 sec. 6.9 /
 * RFC 9113 sec. 6.9.
 * <p>
 * Windows are carried as signed {@code long} so intermediate math can
 * represent the transient negative states RFC 9113 sec. 6.9.2 permits after
 * a {@code SETTINGS_INITIAL_WINDOW_SIZE} decrease. State (current window
 * values) lives on {@link Http2Stream} (per-stream) and
 * {@code Http2ConnectionContext} (connection-level); these helpers only do
 * the math.
 * <p>
 * The RFC caps flow-control windows at {@code 2^31 - 1}; {@link #credit} and
 * {@link #adjustOnInitialWindowChange} return {@link #OVERFLOW} when a
 * requested operation would push the window past that limit. The caller
 * maps the sentinel to {@code FLOW_CONTROL_ERROR} at the appropriate
 * scope (stream-level for per-stream {@code WINDOW_UPDATE} / per-stream
 * adjustment, connection-level for connection-window {@code WINDOW_UPDATE}
 * / a negative-to-positive connection adjustment — though the connection
 * window does not participate in the INITIAL_WINDOW_SIZE delta path per
 * §7).
 */
public final class Http2FlowController {

    /**
     * Upper bound for a flow-control window per RFC 7540 sec. 6.9.1
     * ({@code 2^31 - 1}).
     */
    public static final long WINDOW_MAX = 0x7FFFFFFFL;

    /**
     * Sentinel returned by {@link #credit} and
     * {@link #adjustOnInitialWindowChange} when the requested operation would
     * push the window past {@link #WINDOW_MAX}. Distinct from every legal
     * window value (both bounds of a {@code long} are outside the legal
     * window range).
     */
    public static final long OVERFLOW = Long.MIN_VALUE;

    private Http2FlowController() {
    }

    /**
     * Adjusts {@code window} by a signed {@code delta} produced by a
     * {@code SETTINGS_INITIAL_WINDOW_SIZE} change (RFC 9113 sec. 6.9.2).
     * A negative result is not an error — the stream simply cannot send /
     * receive new {@code DATA} on this direction until a subsequent
     * {@code WINDOW_UPDATE} catches up. A result above {@link #WINDOW_MAX}
     * returns {@link #OVERFLOW}; the caller maps that to a connection
     * {@code FLOW_CONTROL_ERROR}.
     * <p>
     * The apply-initial-window-delta sweep in §7 must run this helper on
     * every direction-active stream as a pre-check before committing any
     * change, so that an overflow aborts with {@code GOAWAY} before any
     * stream window is mutated.
     */
    public static long adjustOnInitialWindowChange(long window, long delta) {
        long result = window + delta;
        if (result > WINDOW_MAX) {
            return OVERFLOW;
        }
        return result;
    }

    /**
     * Returns {@code window + n} or {@link #OVERFLOW} if the result exceeds
     * {@link #WINDOW_MAX}. Used on inbound {@code WINDOW_UPDATE} (when we
     * credit our send window in response to the peer's window grant) and on
     * our own outbound coalesced {@code WINDOW_UPDATE} emission paths.
     * <p>
     * {@code n} must be positive; RFC 7540 sec. 6.9.1 forbids {@code 0}
     * increments and the frame reader rejects them before the payload
     * reaches this helper. Callers that construct their own increment
     * values (coalesced credit-back) must enforce the positive constraint
     * themselves.
     */
    public static long credit(long window, long n) {
        long result = window + n;
        if (result > WINDOW_MAX) {
            return OVERFLOW;
        }
        return result;
    }

    /**
     * Returns {@code window - n}. A negative result after debit is an
     * error on the peer-initiated debit path (DATA exceeds window =
     * {@code FLOW_CONTROL_ERROR}) but legal on the post-SETTINGS adjustment
     * path; the scope decision is the caller's.
     */
    public static long debit(long window, long n) {
        return window - n;
    }
}
