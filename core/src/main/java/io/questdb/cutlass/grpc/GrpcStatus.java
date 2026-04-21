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

package io.questdb.cutlass.grpc;

/**
 * gRPC status codes used by the Flight SQL dispatcher. Values and names
 * match the canonical gRPC status enum
 * (<a href="https://github.com/grpc/grpc/blob/master/doc/statuscodes.md">status codes</a>)
 * so tooling that lists canonical names lines up with ours.
 * <p>
 * Wave 5 only emits a narrow subset; the remaining codes are declared
 * for completeness because they are cheap and future waves (auth,
 * cancellation, prepared statements) will use them without another
 * constant-file revision.
 */
public final class GrpcStatus {

    public static final int CANCELLED = 1;
    public static final int INTERNAL = 13;
    public static final int INVALID_ARGUMENT = 3;
    public static final int OK = 0;
    public static final int PERMISSION_DENIED = 7;
    public static final int RESOURCE_EXHAUSTED = 8;
    public static final int UNAUTHENTICATED = 16;
    public static final int UNIMPLEMENTED = 12;
    public static final int UNKNOWN = 2;

    private GrpcStatus() {
    }

    public static String nameOf(int status) {
        switch (status) {
            case OK:
                return "OK";
            case CANCELLED:
                return "CANCELLED";
            case UNKNOWN:
                return "UNKNOWN";
            case INVALID_ARGUMENT:
                return "INVALID_ARGUMENT";
            case PERMISSION_DENIED:
                return "PERMISSION_DENIED";
            case RESOURCE_EXHAUSTED:
                return "RESOURCE_EXHAUSTED";
            case UNIMPLEMENTED:
                return "UNIMPLEMENTED";
            case INTERNAL:
                return "INTERNAL";
            case UNAUTHENTICATED:
                return "UNAUTHENTICATED";
            default:
                return "UNKNOWN_" + status;
        }
    }
}
