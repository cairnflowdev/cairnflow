// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: BUSL-1.1
// Ports of the two-node console row: node a on `CAIRN_E2E_PORT`
// (default 18560), node b on port + 2; each node also uses port + 1 as its internal port.
export const TWONODE_A = Number(process.env.CAIRN_E2E_PORT ?? 18560);
export const TWONODE_B = TWONODE_A + 2;
