---
name: vector-api-codegen
description: >
  Generate JVM bytecode that invokes the Java Vector API
  (jdk.incubator.vector) for SIMD-accelerated SQL filter evaluation. Use
  when implementing or debugging Vector API-backed filter code generation,
  lane-wise comparisons, masks, type conversions, null handling, row-ID
  output, or exact emitted method signatures.
allowed-tools: Read, Grep, Glob, Bash, Edit, Write, Agent
---

# Vector API Codegen Skill

Guide for generating JVM bytecode that invokes Java Vector API
(`jdk.incubator.vector`) for SIMD-accelerated SQL filter evaluation.

Consult `references/patterns.md` for the complete catalog of validated
Vector API patterns: memory loading, comparisons, mask operations, type
conversions, null handling, row-ID output, and the exact method signatures
the bytecode generator must emit.

Consult `references/gotchas.md` for critical pitfalls discovered during
testing: lane count mismatches, null sentinel coercion, buffer sizing,
and species relationships.
