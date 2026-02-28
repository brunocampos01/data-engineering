# DAX Language — Ticket Management Data Model

DAX formulas for a Power BI star schema tracking a software support/ticket system. Used in conjunction with Databricks or direct Power BI Premium datasets.

---

## Files

| File | Contents |
|------|----------|
| `calculated_column.dax` | Row-context columns added to fact and dimension tables |
| `measures.dax` | Aggregation measures — KPIs, SLA, productivity, time |

---

## Naming Conventions

| Prefix | Type | Example |
|--------|------|---------|
| `f` | Fact table | `fChamadoSaida`, `fChamadoBacklog` |
| `d` | Dimension table | `dTempo`, `dChamado`, `dUsuario` |
| `\|` separator | Table-scoped measure | `fChamadoSaida \| Qtde Entregas` |

---

## Recurring Patterns

### 1 — Parsing dates stored as `YYYYMMDD` integers

Source systems store dates as 8-digit integers (e.g. `20240315`). Every fact table has a calculated column to convert them:

```dax
Data IDDATA :=
    DATEVALUE(
        MID(fChamadoApontamento[IDDATA], 1, 4) & "/" &
        MID(fChamadoApontamento[IDDATA], 5, 2) & "/" &
        MID(fChamadoApontamento[IDDATA], 7, 2)
    )
```

Sentinel value `01/01/1900` (integer `19000101`) signals a missing/null date and is mapped to `BLANK()` or used as an SLA-absent indicator.

### 2 — Working-days count between two dates

`dTempo[Binário Flag Dia da Semana]` is `1` on business days and `0` on weekends/holidays. Summing it between two dates gives elapsed business days, minus 1 to avoid double-counting the start day:

```dax
Tempo Gasto :=
    IF(
        CALCULATE(
            SUM(dTempo[Binário Flag Dia da Semana]),
            DATESBETWEEN(dTempo[Data],
                fChamadoTempoMedio[Data IDDTINICIOSTATUS],
                fChamadoTempoMedio[Data IDDTFINALSTATUS])
        ) = 0,
        0,
        CALCULATE(
            SUM(dTempo[Binário Flag Dia da Semana]),
            DATESBETWEEN(dTempo[Data],
                fChamadoTempoMedio[Data IDDTINICIOSTATUS],
                fChamadoTempoMedio[Data IDDTFINALSTATUS])
        ) - 1
    )
```

### 3 — SLA classification

Three mutually exclusive states are assigned per ticket based on the appraisal period vs. deadline:

| State | Condition |
|-------|-----------|
| `Sem SLA` | Deadline = sentinel `01/01/1900` |
| `SLA a Vencer` | Deadline ≥ today (current month) or ≥ appraisal date (past months) |
| `SLA Vencido` | All other cases |

Binary flags (`SLA a Vencer`, `SLA Vencido`, `Sem SLA` = 0/1) are then summed in measures for aggregation.

### 4 — Row-level security

Filters each fact table to rows belonging to the currently logged-in Power BI user, resolved via `LOOKUPVALUE` against `dUsuario`:

```dax
-- Applied as a role filter on fChamadoApontamento
= fChamadoApontamento[IDUSUARIO] =
    LOOKUPVALUE(dUsuario[IDUSUARIO], dUsuario[Login], [Usuário Logado])

-- Helper measures to extract the login from the Windows username
Usuário Nome    := MID(USERNAME(), SEARCH("\", USERNAME()), 100)
Usuário Logado  := RIGHT([Usuário Nome], LEN([Usuário Nome]) - 1)
```

---

## Key Measures (measures.dax)

### Time dimension (`dTempo`)

| Measure | Description |
|---------|-------------|
| `Dias Úteis Mês` | Business days since 2017, excluding holidays, up to today |
| `Horas Úteis Mês` | `Dias Úteis Mês * 8` |
| `Meses Contabilizados` | Distinct period codes from 2017 to present |

### Throughput & productivity (`fChamadoSaida`)

| Measure | Formula |
|---------|---------|
| `Qtde Entregas` | Resolved tickets, excluding "Defeito Consequência" type |
| `Qtde Entradas` | New tickets in the period |
| `IAD` | `Qtde Entregas / Qtde Entradas` — throughput ratio |
| `Produtividade` | `Qtde Entregas / Qtde Analistas` |
| `Produtividade Mês` | `Qtde Entregas / Dias Úteis Mês` |

### SLA compliance (`fChamadoSaida`)

| Measure | Formula |
|---------|---------|
| `SLA` | `Qtde Entregas No Prazo / (No Prazo + Fora Prazo)` |
| `Qtde Entregas No Prazo` | `No Prazo = 1`, excluding "Defeito Consequência" |
| `Qtde Entregas Fora Prazo` | `Fora Prazo = 1`, excluding "Defeito Consequência" |

> Tickets in lifecycle phases "Release Transition", "Homologação", or "Homologação do Cliente" (`IDFASECICLO` = 5, 11, 19) are always counted as on time.

### Backlog (`fChamadoBacklog`)

| Measure | Description |
|---------|-------------|
| `Qtde Backlog` | Open tickets (excluding consequence defects) |
| `Qtde A Vencer` | Backlog items with upcoming SLA deadline |
| `Qtde Vencidos` | Backlog items past SLA deadline |
| `Qtde Sem SLA` | Backlog items with no SLA defined |

### Time-to-resolution (`fChamadoTempoMedio`)

| Measure | Description |
|---------|-------------|
| `Tempo Médio Solução` | `AVERAGE` of business days per ticket status phase |
| `fChamadoTempoMedio \| Media Tempo Gasto Status` | `SUM(Tempo Gasto) / Qtde Itens` per status |
