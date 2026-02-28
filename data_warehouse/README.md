# Data Warehouse

Reference material and Power BI (DAX) examples for dimensional data warehouse modeling, focused on a ticket/call management system.

---

## Contents

| Path | Description |
|------|-------------|
| `dax_language/calculated_column.dax` | DAX calculated columns for fact and dimension tables |
| `dax_language/measures.dax` | DAX measures — KPIs, SLA, productivity metrics |
| `data_warehouse.ipynb` | Conceptual notes on dimensional modeling and goal-setting |
| `images/` | Visual aids: DW phases, dimensional modeling diagram, BI infographic, mind map |

---

## Data Model — Star Schema (Ticket Management)

The DAX files implement a Power BI star schema for a support/ticket system.

### Fact Tables (`f*`)

| Table | Description |
|-------|-------------|
| `fChamadoApontamento` | Time logged per ticket (hours apportioned) |
| `fChamadoEntrada` | Ticket inflow (new entries) |
| `fChamadoSaida` | Ticket outflow (resolved/delivered) |
| `fChamadoBacklog` | Open backlog snapshot |
| `fChamadoBacklogZero` | Backlog zeroing events |
| `fChamadoNegociado` | Negotiated/rescheduled tickets |
| `fChamadoArtefatoEvolucao` | Artifact evolution history |
| `fChamadoTempoMedio` | Cycle time per ticket status phase |

### Dimension Tables (`d*`)

| Table | Description |
|-------|-------------|
| `dChamado` | Ticket master data (tags, version, priority) |
| `dTempo` | Calendar/time dimension (working days, holidays, weekends) |
| `dTempoApontamento` | Time dimension for logged hours |
| `dTipoChamado` | Ticket type (e.g. "Defeito Consequência") |
| `dCliente` | Client dimension |
| `dUsuario` | User/analyst dimension (used for row-level security) |

---

## Key DAX Patterns

### Date Parsing from Integer (`YYYYMMDD`)
Raw date columns are stored as 8-digit integers. A recurring pattern converts them to proper `DATE` values:

```dax
Data IDDATA :=
    DATEVALUE(
        MID(fChamadoApontamento[IDDATA], 1, 4) & "/" &
        MID(fChamadoApontamento[IDDATA], 5, 2) & "/" &
        MID(fChamadoApontamento[IDDATA], 7, 2)
    )
```

### SLA Classification
Tickets are classified into SLA states based on the appraisal period and deadline:

```dax
-- States: "Sem SLA" | "SLA a Vencer" | "SLA Vencido"
Tipo SLA :=
    IF(fChamadoBacklog[Data IDDTPRAZOSLA] = DATEVALUE("01/01/1900"),
        "Sem SLA",
        IF(AND(MONTH(...) = MONTH(TODAY()), ... >= TODAY()),
            "SLA a Vencer",
            ...
        )
    )
```

### Working Days Calculation
Uses a binary flag on `dTempo` to count business days between two dates, excluding weekends and holidays:

```dax
Tempo Gasto :=
    CALCULATE(
        SUM(dTempo[Binário Flag Dia da Semana]),
        DATESBETWEEN(dTempo[Data],
            fChamadoTempoMedio[Data IDDTINICIOSTATUS],
            fChamadoTempoMedio[Data IDDTFINALSTATUS])
    ) - 1
```

### Row-Level Security
Restricts data by the logged-in Power BI user via `LOOKUPVALUE` against the user dimension:

```dax
-- Role filter on fChamadoApontamento
= fChamadoApontamento[IDUSUARIO] =
    LOOKUPVALUE(dUsuario[IDUSUARIO], dUsuario[Login], [Usuário Logado])
```

### Key KPIs (measures.dax)

| Measure | Formula |
|---------|---------|
| `SLA` | `Qtde Entregas No Prazo / (No Prazo + Fora Prazo)` |
| `IAD` | `Qtde Entregas / Qtde Entradas` (throughput ratio) |
| `Produtividade` | `Qtde Entregas / Qtde Analistas` |
| `Produtividade Mês` | `Qtde Entregas / Dias Úteis Mês` |
| `Horas Úteis Mês` | `Dias Úteis Mês * 8` |
| `Meses Contabilizados` | `DISTINCTCOUNT` of period codes (from 2017 onward) |

---

## Visual Resources (`images/`)

- `fases_dw.png` — Data warehouse implementation phases
- `modelagem_dimensional.png` — Dimensional modeling overview
- `mapa-mental-data-warehouse.jpeg` — Data warehouse concept mind map
- `infografico-o-que-e-bi.pdf` — Business Intelligence infographic
