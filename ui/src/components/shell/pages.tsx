// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Shared shell pages: 404, not permitted, placeholder for screens not built yet.
import { Link } from "@tanstack/react-router";
import { Construction, Lock, SearchX } from "lucide-react";
import { EmptyState } from "@/components/cairn/empty-state";
import { m } from "@/paraglide/messages";

export function NotFound() {
  return (
    <EmptyState
      icon={<SearchX />}
      title={m.not_found_title()}
      action={
        <Link to="/" className="text-primary underline-offset-4 hover:underline">
          {m.not_found_home()}
        </Link>
      }
    >
      {m.not_found_body()}
    </EmptyState>
  );
}

export function Forbidden() {
  return (
    <EmptyState icon={<Lock />} title={m.forbidden_title()}>
      {m.forbidden_body()}
    </EmptyState>
  );
}

export function Placeholder({ title }: { title: string }) {
  return (
    <div className="p-6">
      <h1 className="text-xl font-semibold">{title}</h1>
      <EmptyState icon={<Construction />} title={title}>
        {m.placeholder_body()}
      </EmptyState>
    </div>
  );
}
