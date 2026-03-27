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