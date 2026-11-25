// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Local saved views: open, save the current view, delete.
import { useNavigate } from "@tanstack/react-router";
import { Bookmark, Plus, Trash2 } from "lucide-react";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { InvocationSearch } from "@/lib/invocation-search";
import { deleteView, loadViews, MAX_NAME, saveView } from "@/lib/saved-views";
import { m } from "@/paraglide/messages";

export function SavedViews({ tenant, search }: { tenant: string; search: InvocationSearch }) {
  const navigate = useNavigate({ from: "/invocations/" });
  const [views, setViews] = useState(() => loadViews(tenant));
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger render={<Button variant="ghost" size="sm" />}>
          <Bookmark aria-hidden />
          {m.saved_views()}
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="min-w-56">
          <DropdownMenuGroup>
            <DropdownMenuLabel>{m.saved_views()}</DropdownMenuLabel>
            {views.length === 0 ? (
              <DropdownMenuItem disabled>{m.saved_views_none()}</DropdownMenuItem>
            ) : (
              views.map((v) => (
                <DropdownMenuItem key={v.name} onClick={() => navigate({ search: v.search })}>
                  {v.name}
                </DropdownMenuItem>
              ))
            )}
          </DropdownMenuGroup>
          <DropdownMenuSeparator />
          {/* Save lives in the menu (no extra button next to the title). */}
          <DropdownMenuItem onClick={() => setOpen(true)}>
            <Plus aria-hidden />
            {m.saved_views_save()}…
          </DropdownMenuItem>
          {views.length > 0 ? (
            <>
              <DropdownMenuSeparator />
              <DropdownMenuGroup>
                {views.map((v) => (
                  <DropdownMenuItem
                    key={v.name}
                    onClick={() => setViews(deleteView(tenant, v.name))}
                  >
                    <Trash2 aria-hidden />
                    {m.saved_views_delete({ name: v.name })}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuGroup>
            </>
          ) : null}
        </DropdownMenuContent>
      </DropdownMenu>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{m.saved_views_save()}</DialogTitle>
          </DialogHeader>
          <form
            className="flex flex-col gap-3"
            onSubmit={(e) => {
              e.preventDefault();
              setViews(saveView(tenant, { name, search }));
              setName("");
              setOpen(false);
            }}
          >
            <Label htmlFor="view-name">{m.saved_views_name()}</Label>
            <Input
              id="view-name"
              value={name}
              maxLength={MAX_NAME}
              onChange={(e) => setName(e.target.value)}
              required
            />
            <DialogFooter>
              <Button type="submit" disabled={name.trim() === ""}>
                {m.saved_views_save()}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </>
  );
}
