// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// The console's SQL editor: CodeMirror 6 + `lang-sql`, autocompletion from the table
// catalogue (`tables.gen.ts`, generated from query-tables.md). Only loaded on the
// Query screen (the route is a separate chunk). Mod-Enter runs the statement.
//
// Mounted in a **shadow root**: CodeMirror's `style-mod` injects `<style>` when
// mounted in the document (blocked by CSP `style-src 'self'`), while inside a shadow
// root it uses `adoptedStyleSheets` (constructed stylesheets, not subject to
// `style-src`). The theme's CSS variables are still inherited across the shadow
// boundary.
import { autocompletion, type Completion, completionKeymap } from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { PostgreSQL, sql } from "@codemirror/lang-sql";
import { bracketMatching, HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { EditorState } from "@codemirror/state";
import { EditorView, keymap, placeholder } from "@codemirror/view";
import { tags as t } from "@lezer/highlight";
import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";
import { TABLES } from "@/data/tables.gen";
import { m } from "@/paraglide/messages";

const SCHEMA: Record<string, Completion[]> = Object.fromEntries(
  TABLES.map((tb) => [
    tb.name,
    tb.columns.map((c) => ({ label: c.name, detail: c.type, info: c.doc, type: "property" })),
  ]),
);

/**
 * Syntax colours from **text** tokens (not chart colours — some are below 3:1 against
 * the background): keywords bold, strings and comments muted, comments italic.
 */
const highlight = HighlightStyle.define([
  { tag: t.keyword, fontWeight: "600", color: "var(--foreground)" },
  { tag: [t.string, t.special(t.string)], color: "var(--muted-foreground)" },
  { tag: t.comment, color: "var(--tertiary-foreground)", fontStyle: "italic" },
  { tag: [t.number, t.bool, t.null], color: "var(--primary)" },
]);

const theme = EditorView.theme({
  "&": { fontSize: "13px", backgroundColor: "var(--card)", color: "var(--foreground)" },
  ".cm-content": {
    fontFamily: "var(--font-mono)",
    caretColor: "var(--foreground)",
    minHeight: "8rem",
  },
  ".cm-gutters": { display: "none" },
  "&.cm-focused": { outline: "2px solid var(--ring)", outlineOffset: "-1px" },
  ".cm-selectionBackground, &.cm-focused .cm-selectionBackground": {
    backgroundColor: "var(--accent)",
  },
  ".cm-tooltip": {
    backgroundColor: "var(--popover)",
    color: "var(--popover-foreground)",
    border: "1px solid var(--border)",
  },
  ".cm-tooltip-autocomplete ul li[aria-selected]": {
    backgroundColor: "var(--accent)",
    color: "var(--accent-foreground)",
  },
  ".cm-placeholder": { color: "var(--tertiary-foreground)" },
});

export type SqlEditorHandle = { insert: (text: string) => void; focus: () => void };

export const SqlEditor = forwardRef<
  SqlEditorHandle,
  { value: string; onChange: (v: string) => void; onRun: () => void; label: string }
>(function SqlEditor({ value, onChange, onRun, label }, ref) {
  const host = useRef<HTMLDivElement>(null);
  const view = useRef<EditorView | null>(null);
  // The latest callback — the editor is built once, it must not capture a stale closure.
  const cb = useRef({ onChange, onRun });
  cb.current = { onChange, onRun };

  // Built once; an outside `value` is synced in the effect below.
  // biome-ignore lint/correctness/useExhaustiveDependencies: built once
  useEffect(() => {
    if (!host.current) return;
    const shadow = host.current.shadowRoot ?? host.current.attachShadow({ mode: "open" });
    const mount = document.createElement("div");
    shadow.replaceChildren(mount);
    const v = new EditorView({
      root: shadow,
      parent: mount,
      state: EditorState.create({
        doc: value,
        extensions: [
          history(),
          bracketMatching(),
          sql({ dialect: PostgreSQL, schema: SCHEMA, upperCaseKeywords: true }),
          autocompletion(),
          syntaxHighlighting(highlight),
          placeholder(m.query_placeholder()),
          keymap.of([
            {
              key: "Mod-Enter",
              run: () => {
                cb.current.onRun();
                return true;
              },
            },
            ...completionKeymap,
            ...historyKeymap,
            ...defaultKeymap,
          ]),
          EditorView.lineWrapping,
          EditorView.contentAttributes.of({ "aria-label": label }),
          EditorView.updateListener.of((u) => {
            if (u.docChanged) cb.current.onChange(u.state.doc.toString());
          }),
          theme,
        ],
      }),
    });
    view.current = v;
    return () => {
      v.destroy();
      view.current = null;
    };
  }, []);

  useEffect(() => {
    const v = view.current;
    if (v && v.state.doc.toString() !== value) {
      v.dispatch({ changes: { from: 0, to: v.state.doc.length, insert: value } });
    }
  }, [value]);

  useImperativeHandle(ref, () => ({
    insert: (text) => {
      const v = view.current;
      if (!v) return;
      const { from, to } = v.state.selection.main;
      v.dispatch({
        changes: { from, to, insert: text },
        selection: { anchor: from + text.length },
      });
      v.focus();
    },
    focus: () => view.current?.focus(),
  }));

  return <div ref={host} className="overflow-hidden rounded-md border border-input" />;
});
