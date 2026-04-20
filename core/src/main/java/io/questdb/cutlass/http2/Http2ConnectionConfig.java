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
 * Static per-connection configuration passed to
 * {@link Http2ConnectionContext} at construction. Every field is final and
 * set by the builder; defaults track the Milestone 1 posture documented in
 * {@code STREAM_STATE_MACHINE.md} §14.1 and {@code HPACK_CODEC.md} §16.1.
 */
public final class Http2ConnectionConfig {

    public final int blockAssemblyScratchBytes;
    public final int headerListPolicyCap;
    public final int headerStagingBytes;
    public final int headerStagingTuples;
    public final int hpackDecoderScratchBytes;
    public final int hpackEncoderBufferBytes;
    public final int hpackPoolCapacityBytes;
    public final int ourHeaderTableSize;
    public final int ourInitialWindowSize;
    public final int ourMaxConcurrentStreams;
    public final int ourMaxFrameSize;
    public final int tombstoneCap;

    private Http2ConnectionConfig(Builder b) {
        this.blockAssemblyScratchBytes = b.blockAssemblyScratchBytes;
        this.headerListPolicyCap = b.headerListPolicyCap;
        this.headerStagingBytes = b.headerStagingBytes;
        this.headerStagingTuples = b.headerStagingTuples;
        this.hpackDecoderScratchBytes = b.hpackDecoderScratchBytes;
        this.hpackEncoderBufferBytes = b.hpackEncoderBufferBytes;
        this.hpackPoolCapacityBytes = b.hpackPoolCapacityBytes;
        this.ourHeaderTableSize = b.ourHeaderTableSize;
        this.ourInitialWindowSize = b.ourInitialWindowSize;
        this.ourMaxConcurrentStreams = b.ourMaxConcurrentStreams;
        this.ourMaxFrameSize = b.ourMaxFrameSize;
        this.tombstoneCap = b.tombstoneCap;
    }

    public static Http2ConnectionConfig defaults() {
        return new Builder().build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private int blockAssemblyScratchBytes = 16 * 1024;
        private int headerListPolicyCap = 16 * 1024;
        private int headerStagingBytes = 16 * 1024;
        private int headerStagingTuples = 600;
        // Strictly greater than headerListPolicyCap per §11 sizing discipline
        // so the stream-error path always fires before the HPACK scratch cap.
        private int hpackDecoderScratchBytes = 24 * 1024;
        // Must exceed HpackEncoder's internal bufferFloor, which scales with
        // maxOutboundFieldBytes (= headerListPolicyCap below) plus HPACK
        // framing overhead. 64 KiB comfortably holds a single max-policy
        // field representation with size-update prefix and leaves room for
        // one complete HEADERS block fragment.
        private int hpackEncoderBufferBytes = 64 * 1024;
        private int hpackPoolCapacityBytes = 4096;
        private int ourHeaderTableSize = 4096;
        // RFC 9113 default per-stream initial window.
        private int ourInitialWindowSize = 65_535;
        // §14.1 Milestone 1 default: high enough for multi-stream client
        // warmups without committing to the full production ceiling.
        private int ourMaxConcurrentStreams = 100;
        private int ourMaxFrameSize = 16_384;
        // §15 open question suggests gRPC-Java's default of 100.
        private int tombstoneCap = 100;

        private Builder() {
        }

        public Http2ConnectionConfig build() {
            return new Http2ConnectionConfig(this);
        }

        public Builder withBlockAssemblyScratchBytes(int v) {
            this.blockAssemblyScratchBytes = v;
            return this;
        }

        public Builder withHeaderListPolicyCap(int v) {
            this.headerListPolicyCap = v;
            return this;
        }

        public Builder withHeaderStagingBytes(int v) {
            this.headerStagingBytes = v;
            return this;
        }

        public Builder withHeaderStagingTuples(int v) {
            this.headerStagingTuples = v;
            return this;
        }

        public Builder withHpackDecoderScratchBytes(int v) {
            this.hpackDecoderScratchBytes = v;
            return this;
        }

        public Builder withHpackEncoderBufferBytes(int v) {
            this.hpackEncoderBufferBytes = v;
            return this;
        }

        public Builder withHpackPoolCapacityBytes(int v) {
            this.hpackPoolCapacityBytes = v;
            return this;
        }

        public Builder withOurHeaderTableSize(int v) {
            this.ourHeaderTableSize = v;
            return this;
        }

        public Builder withOurInitialWindowSize(int v) {
            this.ourInitialWindowSize = v;
            return this;
        }

        public Builder withOurMaxConcurrentStreams(int v) {
            this.ourMaxConcurrentStreams = v;
            return this;
        }

        public Builder withOurMaxFrameSize(int v) {
            this.ourMaxFrameSize = v;
            return this;
        }

        public Builder withTombstoneCap(int v) {
            this.tombstoneCap = v;
            return this;
        }
    }
}
