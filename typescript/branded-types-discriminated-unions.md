# Branded Types & Exhaustive Discriminated Unions for Domain Safety

TypeScript uses a **structural** type system: two types with the same underlying shape are interchangeable, even when they represent completely different domain concepts. Declaring `type UserId = string` and `type OrderId = string` provides zero compile-time protection against swapping arguments in `cancelOrder(userId, orderId)`. Similarly, modeling workflows with flat interfaces and optional flags (`{ status: string; paidAt?: Date; failureReason?: string }`) permits impossible states and scatters defensive `if (!x)` checks across the codebase.

Pairing **Branded Types** (nominal tags via `unique symbol` + boundary smart constructors) with **Exhaustive Discriminated Unions** makes illegal states unrepresentable and turns missing state handlers into compile-time errors—with zero runtime overhead.

## 🚀 Core Guarantees

- **Nominal Safety at Zero Runtime Cost**: Brand tags exist strictly in the type checker and erase completely in emitted JavaScript—no wrapper classes or extra heap allocations.
- **Parse, Don't Validate**: Untrusted inputs are validated once at I/O boundaries by smart constructors, so internal business logic accepts proven domain types without repetitive defensive checks.
- **Compile-Time Exhaustiveness**: Modeling states as a tagged union and terminating `switch` blocks with a `never` check ensures adding a new state immediately flags every unhandled code path during compilation.

---

## 🛠️ Production Patterns

### 1. Branded Primitives with `unique symbol` & Smart Constructors

Use `declare const brand: unique symbol` so brands cannot be forged structurally or collide across modules, and isolate all type narrowing inside validated constructors.

```typescript
declare const __brand: unique symbol;

export type Brand<T, B extends string> = T & { readonly [__brand]: B };

export type UserId = Brand<string, "UserId">;
export type OrderId = Brand<string, "OrderId">;
export type Cents = Brand<number, "Cents">;

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function parseUserId(raw: string): UserId {
  if (!UUID_RE.test(raw)) {
    throw new TypeError(`Invalid UserId: ${raw}`);
  }
  return raw as UserId;
}

export function parseOrderId(raw: string): OrderId {
  if (!UUID_RE.test(raw)) {
    throw new TypeError(`Invalid OrderId: ${raw}`);
  }
  return raw as OrderId;
}

export function cents(amount: number): Cents {
  if (!Number.isSafeInteger(amount) || amount < 0) {
    throw new RangeError(`Cents must be a non-negative safe integer, got ${amount}`);
  }
  return amount as Cents;
}

export function addCents(a: Cents, b: Cents): Cents {
  // Validated sum preserves the Cents invariant and catches integer overflow
  return cents(a + b);
}

// --- Compile-time protection in action ---
function issueRefund(userId: UserId, orderId: OrderId, amount: Cents): void {
  // Safe: userId and orderId can never be accidentally swapped
}

const uid = parseUserId("550e8400-e29b-41d4-a716-446655440000");
const oid = parseOrderId("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

issueRefund(uid, oid, cents(1999)); // ✅ Compiles
// issueRefund(oid, uid, cents(1999)); // ❌ Type 'OrderId' is not assignable to 'UserId'
// issueRefund(uid, oid, 19.99);       // ❌ Type 'number' is not assignable to 'Cents'
```

### 2. Illegal-State-Unrepresentable Machine + `assertNever`

Instead of one interface full of optional properties, give each lifecycle state its own shape under a common discriminant (`kind`) and enforce exhaustiveness with `never`.

```typescript
export type PaymentState =
  | { readonly kind: "pending"; readonly createdAt: Date }
  | { readonly kind: "authorized"; readonly authCode: string; readonly expiresAt: Date }
  | { readonly kind: "captured"; readonly receiptId: string; readonly capturedAt: Date; readonly amount: Cents }
  | { readonly kind: "failed"; readonly code: string; readonly retryable: boolean }
  | { readonly kind: "refunded"; readonly refundId: string; readonly refundedAt: Date };

export function assertNever(value: never): never {
  throw new Error(`Unhandled discriminated union member: ${JSON.stringify(value)}`);
}

export function capturePayment(
  state: PaymentState,
  amount: Cents,
  now = new Date()
): PaymentState {
  if (state.kind !== "authorized") {
    throw new Error(`Cannot capture payment in '${state.kind}' state`);
  }
  if (state.expiresAt <= now) {
    return { kind: "failed", code: "AUTH_EXPIRED", retryable: true };
  }

  // state.authCode is guaranteed to exist here—no non-null assertions needed
  return {
    kind: "captured",
    receiptId: `rcpt_${state.authCode}`,
    capturedAt: now,
    amount,
  };
}

export function formatPaymentSummary(state: PaymentState): string {
  switch (state.kind) {
    case "pending":
      return `Awaiting authorization since ${state.createdAt.toISOString()}`;
    case "authorized":
      return `Authorized (${state.authCode}), expires ${state.expiresAt.toISOString()}`;
    case "captured":
      return `Captured ${state.amount} cents (receipt ${state.receiptId})`;
    case "failed":
      return `Failed [${state.code}] (retryable=${state.retryable})`;
    case "refunded":
      return `Refunded (${state.refundId}) at ${state.refundedAt.toISOString()}`;
    default:
      // Adding a new state (e.g. "disputed") causes a compile error here until handled
      return assertNever(state);
  }
}
```

> **Tip**: Pair branded types with schema libraries like Zod (`z.string().uuid().brand<"UserId">()`) at HTTP and queue boundaries so runtime validation and nominal TypeScript types stay in a single source of truth.

---

## ⚠️ Common Pitfalls to Avoid

1. **Scattering `as UserId` Casts in Application Code**: Using type assertions outside the boundary constructor bypasses validation and lets malformed or swapped strings wear a trusted brand. Only cast inside the dedicated parser/smart constructor.
2. **Using Plain Property Brands (`{ __brand: "UserId" }`)**: String-keyed properties pollute IDE autocomplete with fake fields and can still collide if two packages define the same `__brand` string. Prefer `declare const __brand: unique symbol`.
3. **Omitting `default: return assertNever(x)` in `switch` Statements**: Without a `never` check (or an explicit return type on the function), adding a new union variant silently falls off the end of a `switch` and returns `undefined`.
4. **Mutating the Discriminant Tag In-Place**: Assigning `state.kind = "captured"` on an existing object leaves leftover properties from the prior state and breaks TypeScript's control-flow narrowing. Always construct and return a fresh immutable object on transitions.
5. **Over-Branding Every Primitive**: Branding trivial internal strings (`DisplayName`, `LogMessage`) adds friction without preventing real bugs. Focus brands on **IDs**, **units** (`Cents`, `Seconds`, `Bytes`), and **trust boundaries** (`RawSql`, `SanitizedHtml`).
