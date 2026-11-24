// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Layout of every screen after sign-in: needs `whoami`. `401` ⇒ the sign-in page (remembering
// where you were going); `403` on a child screen ⇒ "not enough permission", not a red error.
import { useSuspenseQuery } from "@tanstack/react-query";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { Forbidden } from "@/components/shell/pages";
import { Shell } from "@/components/shell/shell";
import { ApiError } from "@/data/api";
import { whoamiQuery } from "@/data/session";

export const Route = createFileRoute("/_app")({
  beforeLoad: async ({ context, location }) => {
    try {
      await context.queryClient.ensureQueryData(whoamiQuery);
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) {
        throw redirect({ to: "/login", search: { redirect: location.pathname } });
      }
      throw e;
    }
  },
  component: App,
  errorComponent: ({ error }) =>
    error instanceof ApiError && error.status === 403 ? (
      <Forbidden />
    ) : (
      <p role="alert">{String(error)}</p>
    ),
});

function App() {
  const { data: me } = useSuspenseQuery(whoamiQuery);
  return <Shell me={me} />;
}
