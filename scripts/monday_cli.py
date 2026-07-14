#!/usr/bin/env python3
"""
Monday.com CLI — programmatic access for any session or automation.

Usage:
    python3 scripts/monday_cli.py close <item_id> [--board <id>]
    python3 scripts/monday_cli.py move  <item_id> <group_id> [--board <id>]
    python3 scripts/monday_cli.py status <item_id> <label> [--board <id>]
    python3 scripts/monday_cli.py note   <item_id> <text>  [--board <id>]
    python3 scripts/monday_cli.py list   --board <id> [--owner <name>] [--status <label>]
    python3 scripts/monday_cli.py board  <id>
    python3 scripts/monday_cli.py boards

Auth: MONDAY_API_TOKEN env var (or in .env). Get token at
https://<account>.monday.com/apps/manage/tokens (Admin → Developers → My access tokens).

`close` = set status to Done AND move to the board's Done group in one call.
Default board is DSP Dev Work (18406313526) — override with --board.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests

MONDAY_URL = "https://api.monday.com/v2"
DEFAULT_BOARD = 18406313526  # DSP Dev Work

_ROOT = Path(__file__).resolve().parents[1]


def _load_env() -> None:
    env_path = _ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _token() -> str:
    tok = os.environ.get("MONDAY_API_TOKEN")
    if not tok:
        sys.exit(
            "MONDAY_API_TOKEN not set. Add to .env or export it. "
            "Generate at Admin → Developers → My access tokens."
        )
    return tok


def gql(query: str, variables: dict[str, Any] | None = None) -> dict:
    r = requests.post(
        MONDAY_URL,
        headers={"Authorization": _token(), "API-Version": "2024-01"},
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]


def board_meta(board_id: int) -> dict:
    d = gql(
        """
        query ($b: [ID!]) {
          boards(ids: $b) {
            id name
            columns { id title type settings_str }
            groups  { id title }
          }
        }
        """,
        {"b": [str(board_id)]},
    )
    if not d["boards"]:
        raise RuntimeError(f"Board {board_id} not found or no access.")
    return d["boards"][0]


def find_status_column(meta: dict) -> str:
    status_cols = [c for c in meta["columns"] if c["type"] == "status"]
    for c in status_cols:
        if c["title"].strip().lower() == "status":
            return c["id"]
    for c in status_cols:
        try:
            labels = [l["label"].lower() for l in json.loads(c["settings_str"]).get("labels", [])]
        except Exception:
            labels = []
        if "done" in labels:
            return c["id"]
    if status_cols:
        return status_cols[0]["id"]
    raise RuntimeError(f"No status column on board {meta['id']}.")


def find_done_group(meta: dict) -> str:
    for g in meta["groups"]:
        if g["title"].strip().lower() == "done":
            return g["id"]
    raise RuntimeError(
        f"No 'Done' group on board {meta['id']}. Create one or pass an explicit group_id."
    )


def cmd_close(args: argparse.Namespace) -> None:
    meta = board_meta(args.board)
    status_col = find_status_column(meta)
    done_group = find_done_group(meta)
    col_vals = json.dumps({status_col: {"label": "Done"}})
    d = gql(
        """
        mutation ($b: ID!, $i: ID!, $g: String!, $cv: JSON!) {
          status: change_multiple_column_values(board_id: $b, item_id: $i, column_values: $cv) { id }
          move:   move_item_to_group(item_id: $i, group_id: $g) { id }
        }
        """,
        {"b": str(args.board), "i": str(args.item_id), "g": done_group, "cv": col_vals},
    )
    print(f"closed {args.item_id}: status=Done, group={done_group}")
    print(json.dumps(d, indent=2))


def cmd_move(args: argparse.Namespace) -> None:
    d = gql(
        "mutation ($i: ID!, $g: String!) { move_item_to_group(item_id: $i, group_id: $g) { id } }",
        {"i": str(args.item_id), "g": args.group_id},
    )
    print(f"moved {args.item_id} → {args.group_id}")
    print(json.dumps(d, indent=2))


def cmd_status(args: argparse.Namespace) -> None:
    meta = board_meta(args.board)
    status_col = find_status_column(meta)
    col_vals = json.dumps({status_col: {"label": args.label}})
    d = gql(
        """
        mutation ($b: ID!, $i: ID!, $cv: JSON!) {
          change_multiple_column_values(board_id: $b, item_id: $i, column_values: $cv) { id }
        }
        """,
        {"b": str(args.board), "i": str(args.item_id), "cv": col_vals},
    )
    print(f"set status of {args.item_id} → {args.label}")
    print(json.dumps(d, indent=2))


def cmd_note(args: argparse.Namespace) -> None:
    meta = board_meta(args.board)
    text_col = next(
        (c["id"] for c in meta["columns"] if c["type"] == "text" and c["title"].lower() == "notes"),
        None,
    )
    if not text_col:
        text_col = next((c["id"] for c in meta["columns"] if c["type"] == "text"), None)
    if not text_col:
        sys.exit("No text column on board to write note into.")
    col_vals = json.dumps({text_col: args.text})
    d = gql(
        """
        mutation ($b: ID!, $i: ID!, $cv: JSON!) {
          change_multiple_column_values(board_id: $b, item_id: $i, column_values: $cv) { id }
        }
        """,
        {"b": str(args.board), "i": str(args.item_id), "cv": col_vals},
    )
    print(f"wrote note to {args.item_id} ({text_col})")
    print(json.dumps(d, indent=2))


def cmd_list(args: argparse.Namespace) -> None:
    meta = board_meta(args.board)
    people_col = next((c["id"] for c in meta["columns"] if c["type"] == "people"), None)
    status_col = find_status_column(meta)
    cursor = None
    rows: list[dict] = []
    while True:
        page = gql(
            """
            query ($b: [ID!], $c: String) {
              boards(ids: $b) {
                items_page(limit: 500, cursor: $c) {
                  cursor
                  items {
                    id name
                    group { id title }
                    column_values { id text }
                  }
                }
              }
            }
            """,
            {"b": [str(args.board)], "c": cursor},
        )
        ip = page["boards"][0]["items_page"]
        rows.extend(ip["items"])
        cursor = ip.get("cursor")
        if not cursor:
            break
    for it in rows:
        cv = {c["id"]: c["text"] for c in it["column_values"]}
        status_val = cv.get(status_col, "")
        owners_val = cv.get(people_col, "") if people_col else ""
        if args.owner and args.owner.lower() not in (owners_val or "").lower():
            continue
        if args.status and status_val != args.status:
            continue
        print(f"{it['id']}\t[{status_val}]\t{it['group']['title']}\t{it['name']}\t{owners_val}")


def cmd_board(args: argparse.Namespace) -> None:
    meta = board_meta(args.item_id)  # reused positional
    print(f"Board {meta['id']}: {meta['name']}")
    print("\nGroups:")
    for g in meta["groups"]:
        print(f"  {g['id']}\t{g['title']}")
    print("\nColumns:")
    for c in meta["columns"]:
        settings = ""
        if c["type"] == "status":
            try:
                s = json.loads(c["settings_str"])
                settings = " labels=" + ",".join(l["label"] for l in s.get("labels", []))
            except Exception:
                pass
        print(f"  {c['id']}\t{c['type']}\t{c['title']}{settings}")


def cmd_boards(args: argparse.Namespace) -> None:
    d = gql("query { boards(limit: 100, state: active) { id name } }")
    for b in d["boards"]:
        print(f"{b['id']}\t{b['name']}")


def main() -> None:
    _load_env()
    p = argparse.ArgumentParser(description="Monday.com CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("close", help="Status→Done + move to Done group")
    c.add_argument("item_id", type=int)
    c.add_argument("--board", type=int, default=DEFAULT_BOARD)
    c.set_defaults(func=cmd_close)

    c = sub.add_parser("move", help="Move item to a group")
    c.add_argument("item_id", type=int)
    c.add_argument("group_id")
    c.add_argument("--board", type=int, default=DEFAULT_BOARD)
    c.set_defaults(func=cmd_move)

    c = sub.add_parser("status", help="Set status label")
    c.add_argument("item_id", type=int)
    c.add_argument("label")
    c.add_argument("--board", type=int, default=DEFAULT_BOARD)
    c.set_defaults(func=cmd_status)

    c = sub.add_parser("note", help="Write into first text column (or 'Notes')")
    c.add_argument("item_id", type=int)
    c.add_argument("text")
    c.add_argument("--board", type=int, default=DEFAULT_BOARD)
    c.set_defaults(func=cmd_note)

    c = sub.add_parser("list", help="List items, optional owner/status filter")
    c.add_argument("--board", type=int, default=DEFAULT_BOARD)
    c.add_argument("--owner", default=None)
    c.add_argument("--status", default=None)
    c.set_defaults(func=cmd_list)

    c = sub.add_parser("board", help="Show board groups + columns")
    c.add_argument("item_id", type=int, metavar="board_id")
    c.set_defaults(func=cmd_board)

    c = sub.add_parser("boards", help="List all accessible boards")
    c.set_defaults(func=cmd_boards)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
