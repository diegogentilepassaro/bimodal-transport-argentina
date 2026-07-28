# PROBLEMAS detectados en el Paso 3 — log trazable (2026-07-15)

> Registro honesto de los problemas hallados, su relación con el MÉTODO y qué se hizo. Complementa
> `README.md` (protocolo/algoritmo) y `ROADMAP.md` (plan/estado). El crudo `poblados_1960.csv` nunca se tocó.

## Contexto: qué se corrió antes de detectarlos
3.0 provincias ✓ · 3.1 crosswalk de deptos por **name-match** (442 identidad + 45 flags resueltos con
fuente) · motor extendido · 2 pilotos · **corrida masiva 3.2** sobre 3.063 →
`auto_ok` 1915 (62.5%) · `flag_bahra` 530 · `flag_sin_match` 341 · `flag_variante` 200 · `flag_depto` 76 ·
`flag_ambiguo` 1.

## PROBLEMA RAÍZ (de método)
**3.1 testeó PERSISTENCIA DE NOMBRE, no PERSISTENCIA TERRITORIAL.** Marcó `identidad` a todo depto 1960
cuyo nombre sobrevive en Georef. Pero un departamento puede **conservar el nombre y subdividirse**
(ceder territorio a un hijo nuevo). ⇒ los splits con **padre sobreviviente** quedaron **invisibles** al
name-match; solo se descubrían de casualidad en 3.2 (`flag_depto`), de forma incompleta y **mezclados con
homónimos**.

**Medición del punto ciego:** de **46** departamentos padres que resultaron estar subdivididos (tenían
localidades en `flag_depto`), **42 estaban marcados `identidad`** por 3.1.

## Tabla de problemas
| # | Problema | Relación con el método | Alcance | Estado |
|---|---|---|---|---|
| **P1** | Splits con padre-sobreviviente **no modelados** | 3.1 = match de nombre, no de historia territorial | 42 padres; ~76 loc observadas (posible subestimación: solo se ven las loc que matchean exacto en el hijo) | **Se corrige con 3.1b** (historia sistemática con fuente) |
| **P2** | `flag_depto` **mezcla** splits reales + transferencias de límite + homónimos | Sin historia ni geografía, "nombre exacto en otro depto" no distingue hijo-de-split de tocayo lejano | ~12/76 sospechosos de homónimo (p.ej. `Pavón` Exaltación de la Cruz→General Lavalle ~300 km; `San José` Necochea→Coronel Suárez; Río Cuarto→San Javier; Tinogasta→Fray M. Esquiú) | 3.1b acota; residuo → **discriminador de distancia entre centroides** + 3.3 humano |
| **P3** | **Typo** en la resolución manual de 3.1 | Error humano de tipeo; **lo cazó el loop `flag_depto`** (beneficio del método) | La Rioja `Vélez Sarsfield`: se puso "General Ángel Vicente Peñaloza"; Georef = **"Ángel Vicente Peñaloza" [46056]** → 2 loc en flag_depto | **A corregir en la reconstrucción del crosswalk** |
| **P4** | Riesgo **residual** de `auto_ok` errado por homónimo+split | Si la loc migró al hijo y queda un tocayo con el mismo nombre en el padre, el match exacto en el padre daría el lugar equivocado | Bajo; no cuantificable sin geografía | Mitigado al modelar splits (3.1b) |
| **P5** | `flag_sin_match` (341) = problema de **NOMBRE de localidad**, no de depto | Ortogonal a la historia de deptos (prefijos "Balneario/Colonia/Villa", grafía, o ausencia de la capa censal) | 341 (309 sin sugerencia, 32 con pista fuzzy) | Va a **3.3a** (2ª pasada Georef + investigación web con fuente) |

## Decisión de método (José, 2026-07-15)
Se evaluó relajar el match a "localidad+provincia, departamento solo para homónimos"
(*department-last*). **Se DESCARTÓ.** Motivo, probado con nuestros propios datos: si la localidad 1960
**no está en Georef** pero existe un **tocayo** en otra parte de la provincia, ese método la
auto-aceptaría al lugar equivocado **en silencio** (`Pavón` → Pavón/General Lavalle, ~300 km). El
departamento es la **única verificación independiente** de que el match es el lugar correcto.

⇒ **Se mantiene el match exacto en los 3 campos**, pero **después de acomodar la historia**: el crosswalk
define un **CONJUNTO PERMITIDO** de deptos modernos por depto 1960 (`identidad`/`rename` → 1;
`split` → {padre + hijos}) y se exige `depto_georef ∈ conjunto`. Los splits dejan de dar flags falsos;
los tocayos lejanos siguen flageando.

**Principio subyacente:** el costo del método elegido es **TRABAJO** (revisar flags); el del descartado,
**DATOS MALOS SILENCIOSOS**. Todo el protocolo del proyecto (Paso 2: la capa automática solo PARTE, nunca
decide; nada en silencio) prefiere trabajo antes que error silencioso.

## Qué se rehace y qué no
- **De cero:** `crosswalk_indec.csv` (con historia completa 3.1b + P3 corregido) y `geo_match_simple.csv`
  (3.2 re-corrida entera con la regla de conjunto; se descarta la salida anterior).
- **No se rehace:** 3.0 provincias (verificado); el motor `geocode_georef.py` (solo cambia la regla de
  decisión); `georef_cache.json` (respuestas **crudas** de API con **clave de firma completa** ⇒
  algoritmo-independiente: no contamina y abarata el re-run; una corrida en frío daría el mismo resultado).
- **Re-pilotar** la regla nueva antes del masivo; **diff QC** viejo vs nuevo (esperado: splits
  `flag_depto`→`auto_ok`; los 1915 `auto_ok` estables; residuo = homónimos/transferencias).

## RESULTADO DE LA CORRECCIÓN (v2) — verificado
**3.1b corrido:** 30 creaciones post-1960 en 6 provincias (BA 18, Tucumán 6, Entre Ríos 3, Chaco 1,
Jujuy 1, TdF 1); **17 provincias 1:1 limpias**. Todo con `fuente_url` (BA verificado contra el texto de
ley en `normas.gba.gob.ar`).

**Fuente primaria hallada:** codebook CELADE del censo 1960 (`ref/ar60divp.pdf`) — enumera los deptos
existentes en 1960 por provincia. Permite comparar *lista oficial 1960 vs moderna* en vez de inferir
fechas de creación. **Validó el Paso 1B: 18/23 provincias con conteo EXACTO**, y confirmó de forma
independiente 3 problemas nuestros (artefacto `Chubut`, anomalía `Caseros`/La Pampa, doble-grafía
`9 de Julio`/`Nueve de Julio` en Santa Fe) + que `Isla Martín García` y `Zona Nacional Puerto La Plata`
eran unidades censales oficiales 1960.

**`crosswalk_indec.csv` v2:** 529 filas / 487 deptos 1960; **32 deptos con conjunto múltiple**;
0 sin resolver; **0 filas no-identidad sin fuente**; doble corrida idéntica. P3 corregido.

**Re-corrida masiva 3.2 (v2) + diff QC contra v1 — cada cambio explicado:**
| estado | v1 | v2 | Δ |
|---|---|---|---|
| `auto_ok` | 1915 | **1956** | +41 |
| `flag_depto` | 76 | **35** | **−41** |
| `flag_sin_match` | 341 | 321 | −20 |
| `flag_bahra` | 530 | 543 | +13 |
| `flag_variante` | 200 | 207 | +7 |

Solo 3 transiciones (3002 filas sin cambio): `flag_depto→auto_ok` (41, = los splits documentados),
`flag_sin_match→flag_bahra` (13) y `→flag_variante` (7) (la cascada/fuzzy ahora filtran por el conjunto,
que incluye los hijos). **0 `auto_ok` cambió su `georef_id`** ⇒ ninguna coordenada previa se alteró.

**Residuo `flag_depto` = 35**, ordenado por distancia entre centroides: **<60 km** = transferencia de
límite plausible (Villars→Las Heras 17; Calderón→Cnel Rosales 41; Arroyo Dulce→Salto 53; Pasteur→Lincoln
55; Tucumán Famaillá→Leales 46×3); **≥150 km** = homónimo probable (perfil de nombre genérico: `San José`
×3, `San Antonio`×2, `San Pedro`, `Santo Domingo`, `Pavón` 330…). Caso sistemático a mirar: Santa Fe
`General Obligado`→`Vera`, **5 localidades a 93 km** — 5 casos juntos no son 5 tocayos independientes;
probable límite real entre dos deptos grandes.

## P6 — Caracteres invisibles en Georef rompían el match (detectado 2026-07-15, tras la corrida v2)
**Síntoma:** José preguntó *"los flag_bahra, ¿por qué estarían mal? revisemos"*. Al mirar los `flag_bahra`
grandes saltó lo absurdo: **Junín (53.489 hab) y Olavarría (35.107)** son ciudades importantes — tienen
que estar en `localidades-censales`, no en BAHRA.

**Causa raíz:** los datos de Georef traen un **mojibake sistemático**: un **U+00AD (SOFT HYPHEN,
invisible)** incrustado **después de la letra `í`**. Bytes reales de Georef para Junín:
`4a 75 6e ED AD 6e` = `J u n í [U+00AD] n`. Para el servidor `"Junín" != "Juní­n"` ⇒ `exacto=true`
devuelve **0 hits** ⇒ la localidad caía a la capa BAHRA (o a `sin_match`) **en silencio**.

**Alcance medido** (`scripts/diag_bahra.py`, sobre los 1071 flags bahra/variante/sin_match):
**18 mal clasificados, todos `flag_bahra`, todos por el invisible — 125.463 habitantes.** Patrón único
(`í`+U+00AD): `Juní­n`, `Olavarrí­a`, `Frí­as`, `Loberí­a`, `Marí­a Ignacia`, `Santa Lucí­a`,
`Salvador Marí­a`, `El Paraí­so`, `Agustí­n Roca`, `Fortí­n Olavarría`, `Benjamí­n Gould`, `Chavarrí­a`…

**Auditoría general** (`scripts/audit_caracteres.py`) — para no parchear a ciegas:
| Universo | Resultado |
|---|---|
| Georef (2867 nombres vistos) | **solo U+00AD, 28 ocurrencias**; nada más; todo NFC |
| Nuestro canon (2971) | **limpio**, todo NFC |
| Crosswalk `depto_moderno` (429, a mano) | **limpio** |

**Causa raíz REAL (arquitectural):** delegamos la **igualdad de nombres** a `exacto=true`, es decir a la
comparación de strings **del servidor sobre datos que no controlamos**. El bug de datos de Georef se
convertía en un error de clasificación nuestro, silencioso.

**Bug latente del mismo tipo (encontrado al arreglar, aún no había mordido):** la pertenencia al conjunto
se evaluaba con `h["depto"] in exp` — **igualdad cruda de strings** entre el depto de Georef y el nuestro.
Un invisible en un nombre de departamento habría fallado igual, en silencio.

**Arreglo (general, no un parche del soft hyphen):**
1. **`norm_name()` en `geocode_georef.py`** = fuente única de verdad: NFKD → quita **toda** la categoría
   Unicode **`Cf`** (cubre U+00AD, zero-width U+200B–200D, BOM — genérico) → quita marcas combinantes
   (acento-insensible, igual que README 3.2(E)) → unifica comillas/guiones → casefold → colapsa espacios.
2. **Se deja de usar `exacto=true` para decidir.** Se consulta **sin `exacto`** (más recall, filtrando por
   cada depto del conjunto) y **la igualdad la decidimos nosotros** con `norm_name()`.
3. **La comparación de departamento también se normaliza.**
**Verificado** (`scripts/test_norm_name.py`): 14 tests unitarios (soft hyphen, zero-width, BOM, NBSP, NFD,
acentos, comillas, guiones) + los 12 casos reales + test negativo de **no-colisión** (`Junín`≠`Junín (Est.)`,
`Tafí del Valle`≠`Tafí Viejo`). Todos pasan.

**Resultado del fix — re-match v3 desde cero (diff QC contra v2):**
| estado | v2 | v3 | Δ |
|---|---|---|---|
| `auto_ok` | 1956 | **1993 (65,1%)** | **+37** |
| `flag_bahra` | 543 | 522 | −21 |
| `flag_variante` | 207 | 192 | −15 |
| `flag_sin_match` | 321 | 318 | −3 |
| `flag_depto` | 35 | 37 | +2 |

Solo 4 transiciones (3023 filas sin cambio), **todas explicadas**: `flag_bahra→auto_ok` **22** (el soft
hyphen de Georef); `flag_variante→auto_ok` **15** (guión↔espacio: `Bell-Ville`=`Bell Ville`,
`Quemú-Quemú`, `Choele-Choel`, `Cutral-Có`, `Picún-Leufú`… mismo topónimo, unificado por la norma
documentada); `sin_match→flag_depto` 2; `sin_match→flag_bahra` 1.
**Seguridad: 0 `auto_ok` cambió de `georef_id`; 0 perdió el estado.**
**Determinismo verificado:** doble corrida (Santa Cruz) idéntica; masivo-BA vs piloto-BA → 0 filas distintas.

**Hallazgo lateral (clave de fila):** `(page, n_orden)` **NO es clave única** — el nº de orden se repite
entre secciones/provincias de una misma página (los **footnote(1) del conurbano están en una sección
aparte con numeración propia**: `v3_p08/25` = `Ensenada` *y* `Florencio Varela`). La clave correcta es
**`(page, n_orden, localidad)`** → 0 duplicados en las 3.063. Una comparación mía usó la clave corta y
colapsaba 1 fila (la "discrepancia de determinismo" que resultó ser un bug del QC, no del matcher).

## P7 — Dos bugs más de Georef, hallados al verificar `flag_bahra` (2026-07-16)
Disparador: pregunta de José — *"los flag_bahra, ¿por qué estarían mal? encontrarlos en BAHRA no es
problema per se. ¿cómo podemos determinar si nos equivocamos antes?"*. Correcto: `flag_bahra` mezclaba
(a) *la localidad genuinamente no es censal* (BAHRA = fuente correcta, **sin error**) vs (b) *nuestra
búsqueda censal falló* (error nuestro = P6). **Test:** post-P6, el tier 1 recorre la capa censal de **todo
el departamento** con `norm_name()` ⇒ si el nombre fuera censal, lo encontraríamos. Por lo tanto hoy
**`flag_bahra` = "el nombre no es una localidad censal"**, no "fallamos". (520/522 con **un solo
candidato**; mediana 299 hab.) Caso que lo prueba: **`Necochea`** — el censal moderno es
`Necochea - Quequén` (INDEC **fusionó**) y BAHRA conserva `Necochea` sola ⇒ para el mapeo 1960 **BAHRA es
la fuente MEJOR**, no un premio consuelo.

**P7.1 — Ids INDEC desalineados ENTRE CAPAS de Georef.** Mismo nombre, **misma coordenada**, **id
distinto** (depto Necochea):
| nombre | capa `localidades` | capa `localidades-censales` |
|---|---|---|
| Juan N. Fernández | id **06581030** · (-38.0055, -59.2639) | id **06581040** · (-38.0055, -59.2641) |
| Necochea - Quequén | id **06581040** · (-38.5545, -58.7392) | id **06581050** · (-38.5545, -58.7393) |
| Nicanor Olivera | id **06581050** | id **06581060** |
La capa `localidades` no lista `Energía` y desde ahí **corre todos los ids un lugar**.
⇒ **El pareo nombre↔coordenada es CORRECTO en ambas capas** (nuestras coordenadas son confiables), pero
el `georef_id` de los **77** `flag_bahra` que vienen de la capa `localidades` **NO es un id censal INDEC
válido** → se marcan `id_no_censal=true`. **Los `auto_ok` NO están afectados** (salen solo de la capa
censal). *(El campo `localidad_censal` queda exonerado: reporta fielmente lo que dice la capa censal.)*

**P7.2 — `campos=completo` devuelve mojibake doble-encodeado**: `"PehuajÃ³"` (=`PehuajÃ³`, no
`Pehuajó`), `"JunÃ­n"` (=`JunÃ­n`). Con `campos` específicos no ocurre ⇒ **PROHIBIDO usar
`campos=completo`** (asentado en el README).

**Lección:** la verificación de un hallazgo (P6) destapó dos bugs más. Vale la pena **auditar la fuente
externa en profundidad una vez**, en lugar de confiar campo por campo cuando hace falta.

## P8 — Asimetría censal/BAHRA en 3.2: faltaba el tier BAHRA-fuzzy (2026-07-16)
Disparador: José pidió *"estudiá las de C5 a ver si hay algo sistemático y determinístico que se pueda
implementar antes de otra cosa"* — es decir, buscar reglas **antes** de mandar 266 flags a investigación
manual. Al hacerlo apareció un hueco del propio algoritmo.

**El defecto:** 3.2 trataba las capas de forma **asimétrica**:
| capa | exacto | fuzzy |
|---|---|---|
| `localidades-censales` | ✅ `auto_ok` | ✅ `flag_variante` |
| BAHRA (`localidades`/`asentamientos`) | ✅ `flag_bahra` | ❌ **FALTABA** |
⇒ un paraje presente en BAHRA con el nombre apenas distinto caía a `flag_sin_match` **sin que nadie lo
mirara**. No había razón para la asimetría: fue un olvido.

**Medición** (`scripts/diag_c5.py`, sobre los 266 `sin_match` sin propuesta):
| hipótesis | n | pob | % pob C5 |
|---|---|---|---|
| **H1 BAHRA-fuzzy in-set** | **67** | 42.473 | 13,8% |
| H3 prefijos faltantes (Desvío/Empalme/Pueblo/Kilómetro) | 4 | 12.670 | 4,1% |
| H2 generador × capas BAHRA | 5 | 2.060 | 0,7% |
| H4 de/del | 0 | — | absorbido por H1 |
| **sin rescate → investigación manual** | **190** | 250.664 | 81,4% |

**Arreglo:** nuevo tier **`flag_variante_bahra`** en 3.2 (BAHRA-fuzzy dentro del conjunto), **después** del
censal-fuzzy (un candidato censal pesa más). H2/H3/H4 van al **generador de 3.3a** (son interpretación,
no comparación — ver LÍMITE DE ALCANCE en README §3.3a).

**Advertencia registrada:** el fuzzy de H1 es **laxo** (acepta cualquier aproximado del depto correcto).
Ej. `Algarrobo` → `Chosoico Algarrobo` casi seguro **no es el mismo lugar**. H1 **no resuelve** 67 casos:
los mueve de *"sin propuesta"* a *"propuesta para revisar"* — varios serán rechazados en 3.3, y está bien.
Aciertos claros: `Sta. Rosa del Río Primero`→`Santa Rosa de Río Primero`; `Concepción del Tío`→`Villa
Concepción del Tío`; `Grumbein`→`Grünbein`; `Gobernador Ingeniero Valentín Virasoro`→`Gobernador Igr.
Valentín Virasoro`.

**Lección:** *buscar la regla antes de moler a mano.* Pedir "¿qué hay de sistemático acá?" antes de 266
búsquedas manuales destapó un defecto del algoritmo. Cada flag que rescata una regla determinística no es
solo trabajo ahorrado: es una decisión **auditable y reproducible** en vez de un juicio suelto.

## Pendientes abiertos (para 3.3)
- **San Juan `Zonda`** (hallazgo de la validación primaria): la lista oficial 1960 tiene un depto `Zonda`;
  nosotros transcribimos 0 localidades ahí, y la localidad `Zonda` (v8_p05/46) está impresa bajo depto
  `'Venticinco de Mayo'`. Georef no tiene localidad censal `Zonda` en San Juan. ¿Misprint del depto en la
  fuente, o paraje real dentro de 25 de Mayo? → **imagen + decisión humana**.
- **Chubut `Chubut`** (artefacto) y **La Pampa `Caseros`** (anomalía): confirmados contra fuente primaria;
  esperan decisión de imagen.
- Los 35 `flag_depto` residuales (transferencia vs homónimo) y los flags de nombre (`bahra`/`variante`/
  `sin_match`) → Paso 3.3a (investigación documentada) + 3.3 (confirmación humana uno por uno).

## Lecciones (para no repetirlas)
1. **"El nombre sobrevive" ≠ "la unidad no cambió".** Al reconciliar unidades administrativas entre dos
   épocas hay que modelar la **historia territorial**, no solo comparar strings.
2. El **loop de realimentación** (`flag_depto`) valió: destapó los 42 splits ocultos **y** un typo humano.
   Conviene diseñar siempre un check que pueda contradecir la tabla que uno mismo construyó.
3. **Estrictez = seguridad.** Aflojar un filtro para reducir flags cambia trabajo por error silencioso.
   En datos de investigación, preferir el flag.
4. Lo que **no** está documentado (transferencias de límite entre deptos preexistentes) hay que
   **declararlo como límite** del método, no disimularlo: queda como residuo caso-a-caso.
5. **No delegar una decisión propia a la comparación de un tercero sobre datos que no controlás** (P6).
   `exacto=true` de Georef parecía una garantía de rigor; en realidad era una caja negra sobre datos
   sucios que convertía su mojibake en errores nuestros, silenciosos. La igualdad de nombres es una
   **decisión del método** ⇒ tiene que estar en nuestro código, documentada y testeada.
6. **Un resultado absurdo vale más que cien plausibles.** El bug no lo encontró ningún test: lo encontró
   notar que *Junín y Olavarría no pueden ser parajes de BAHRA*. Cuando un caso grande y conocido cae en
   una categoría rara, es señal de bug propio — no de rareza del dato. (Lo disparó la pregunta de José
   "¿por qué estarían mal los bahra? revisemos".)
7. **Auditar antes de parchear.** Ante "hay un problema con caracteres especiales", la respuesta no fue
   arreglar el soft hyphen sino **auditar el universo entero** (Georef, canon, crosswalk) y recién ahí
   fijar el normalizador — que quedó genérico (categoría `Cf`) en vez de una lista negra.
