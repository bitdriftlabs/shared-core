// shared-core - bitdrift's common client/server libraries
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_hir;
extern crate rustc_span;

use clippy_utils::{
  diagnostics::span_lint,
  get_parent_expr,
  res::MaybeDef,
  sym::{lock, unwrap},
};
use rustc_hir::{Expr, ExprKind, def::Res};
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::symbol::sym;

dylint_linting::declare_late_lint! {
  /// ### What it does
  ///
  /// Checks that `std::sync::Mutex::lock()` is immediately followed by `unwrap()`.
  ///
  /// ### Why is this bad?
  ///
  /// This workspace treats a poisoned standard mutex as a programming error. Handling its
  /// `LockResult` in another way can accidentally continue with poisoned state.
  ///
  /// ### Example
  ///
  /// ```rust
  /// # use std::sync::Mutex;
  /// # let mutex = Mutex::new(());
  /// let guard = mutex.lock().expect("mutex poisoned");
  /// ```
  ///
  /// Use instead:
  ///
  /// ```rust
  /// # use std::sync::Mutex;
  /// # let mutex = Mutex::new(());
  /// let guard = mutex.lock().unwrap();
  /// ```
  pub MUTEX_LOCK_MUST_UNWRAP,
  Warn,
  "`std::sync::Mutex::lock()` must be immediately unwrapped"
}

impl<'tcx> LateLintPass<'tcx> for MutexLockMustUnwrap {
  fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'_>) {
    if !is_std_mutex_lock(cx, expr) || is_immediately_unwrapped(cx, expr) {
      return;
    }

    span_lint(
      cx,
      MUTEX_LOCK_MUST_UNWRAP,
      expr.span,
      "call `std::sync::Mutex::lock()` as `.lock().unwrap()`",
    );
  }
}

// Resolve the called associated function so aliases, deref coercions, and UFCS calls are handled
// as consistently as ordinary method syntax.
fn is_std_mutex_lock(cx: &LateContext<'_>, expr: &Expr<'_>) -> bool {
  let method_def_id = match expr.kind {
    ExprKind::MethodCall(method, ..) if method.ident.name == lock => {
      cx.typeck_results().type_dependent_def_id(expr.hir_id)
    }
    ExprKind::Call(function, [_]) => match function.kind {
      ExprKind::Path(path) => match cx.qpath_res(&path, function.hir_id) {
        Res::Def(_, def_id) if cx.tcx.item_name(def_id) == lock => Some(def_id),
        _ => None,
      },
      _ => None,
    },
    _ => None,
  };

  method_def_id
    .and_then(|method_def_id| cx.tcx.impl_of_assoc(method_def_id))
    .is_some_and(|impl_def_id| cx.tcx.type_of(impl_def_id).is_diag_item(cx, sym::Mutex))
}

fn is_immediately_unwrapped(cx: &LateContext<'_>, expr: &Expr<'_>) -> bool {
  let Some(parent) = get_parent_expr(cx, expr) else {
    return false;
  };
  let ExprKind::MethodCall(method, receiver, [], _) = parent.kind else {
    return false;
  };

  method.ident.name == unwrap && receiver.hir_id == expr.hir_id
}

#[test]
fn ui() {
  dylint_testing::ui_test(env!("CARGO_PKG_NAME"), "ui");
}
