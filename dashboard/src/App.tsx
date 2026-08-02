import type { CSSProperties } from "react";
import { c, font } from "./tokens";
import { loadSlate, servedOverHttp } from "./loadSlate";

/**
 * Phase 0: pipeline proof, not the design.
 *
 * This screen exists to verify the notebook -> slate.js -> React contract holds
 * end to end, in both `npm run dev` and a double-clicked dist/index.html,
 * BEFORE any of the real layout gets built on top of it. Phase 1 replaces this
 * wholesale with the three-column workspace from README.md.
 */
export default function App() {
  const status = loadSlate();

  if (!status.ok) {
    return (
      <div style={{ padding: 40, fontFamily: font.mono, color: c.red, fontSize: 13 }}>
        <div style={{ marginBottom: 8, letterSpacing: "0.14em" }}>NO SLATE DATA</div>
        <div style={{ color: c.muted, lineHeight: 1.6 }}>{status.detail}</div>
      </div>
    );
  }

  const { meta, players, form, tracker } = status.slate;
  const top = [...players].sort((a, b) => b.P_TOP20 - a.P_TOP20).slice(0, 10);

  return (
    <div style={{ padding: "24px 28px", fontFamily: font.sans, color: c.text }}>
      <div
        style={{
          fontFamily: font.mono,
          fontSize: 10,
          fontWeight: 600,
          letterSpacing: "0.18em",
          color: c.green,
        }}
      >
        PGA SLATE TERMINAL · PHASE 0
      </div>

      <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginTop: 6 }}>
        <div style={{ fontSize: 17, fontWeight: 600, letterSpacing: "-0.01em" }}>
          {meta.tournament}
        </div>
        <div style={{ fontFamily: font.mono, fontSize: 11, color: c.muted }}>
          {meta.course}
        </div>
        <div style={{ fontFamily: font.mono, fontSize: 11, color: c.dim }}>
          {meta.ending_date}
        </div>
      </div>

      <div
        style={{
          fontFamily: font.mono,
          fontSize: 11,
          color: c.dim,
          marginTop: 14,
          lineHeight: 1.7,
        }}
      >
        <div>
          field <span style={{ color: c.text2 }}>{players.length}</span>
          {"  ·  "}form entries{" "}
          <span style={{ color: c.text2 }}>{Object.keys(form ?? {}).length}</span>
          {"  ·  "}tracker rows{" "}
          <span style={{ color: c.text2 }}>{tracker?.length ?? 0}</span>
        </div>
        <div>
          served over http{" "}
          <span style={{ color: servedOverHttp ? c.green : c.amber }}>
            {servedOverHttp ? "yes — golf.db reachable" : "no — file://, Results Browser disabled"}
          </span>
        </div>
        <div>
          generated <span style={{ color: c.text2 }}>{meta.generated_at}</span>
        </div>
      </div>

      <table
        style={{
          marginTop: 20,
          borderCollapse: "collapse",
          fontFamily: font.mono,
          fontSize: 12,
        }}
      >
        <thead>
          <tr style={{ color: c.muted, fontSize: 10, letterSpacing: "0.09em" }}>
            {["PLAYER", "SALARY", "P(TOP-20)", "SG:F", "SG:C"].map((h) => (
              <th
                key={h}
                style={{
                  textAlign: h === "PLAYER" ? "left" : "right",
                  padding: "0 14px 6px 0",
                  borderBottom: `1px solid ${c.lineStrong}`,
                  fontWeight: 400,
                }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {top.map((p) => (
            <tr key={p.PLAYER}>
              <td
                style={{
                  fontFamily: font.sans,
                  fontSize: 13,
                  padding: "6px 14px 6px 0",
                  borderBottom: `1px solid ${c.lineSoft}`,
                }}
              >
                {p.PLAYER}
              </td>
              <td style={cell(c.text2)}>${p.SALARY.toLocaleString()}</td>
              <td style={cell(c.green)}>{(p.P_TOP20 * 100).toFixed(1)}</td>
              <td style={cell(p.SG_FORM >= 0 ? c.greenSoft : c.redSoft)}>
                {p.SG_FORM.toFixed(2)}
              </td>
              {/* exactly 0 means NO course history, not neutral — the handoff
                  calls this distinction out explicitly */}
              <td
                style={cell(
                  p.SG_CH_SHRUNK === 0
                    ? c.dimmer
                    : p.SG_CH_SHRUNK > 0
                      ? c.greenSoft
                      : c.redSoft,
                )}
              >
                {p.SG_CH_SHRUNK.toFixed(2)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function cell(color: string): CSSProperties {
  return {
    textAlign: "right",
    padding: "6px 14px 6px 0",
    borderBottom: `1px solid ${c.lineSoft}`,
    color,
  };
}
