# Digitalización y georreferenciación del Censo de Población 1960 a nivel de localidad

**Insumo para el pipeline de Market Access — informe para Diego**
Autor de esta corrida: José (asistido). Fecha: 2026-07-22 (actualizado tras el cierre del Grupo A del Paso 3.3).
Estado global: **PROVISIONAL** — el **Grupo A** de la confirmación humana (Paso 3.3) ya está **CERRADO**
(vía la pasada web documentada **3.3b**); quedan el **Grupo B** (172, vistazo al mapa), el **Grupo C** (17
sin_coordenada) y la **muestra Q2** del bloque `auto_muestreo` (829). Ver §3 y §6.

> **Cómo leer este informe.** La fuente de verdad es la carpeta local `_local_geocoding_1960/`
> (gitignored, hoy fuera del repo). El protocolo está en `README.md`, el plan/estado en `ROADMAP.md`,
> y los problemas de método en `PROBLEMAS_paso3.md`. **Todos los números de este informe fueron
> verificados en vivo contra los CSV reales** el 2026-07-22 (no copiados de memoria). Donde un número
> del informe difiere de lo que dice la documentación, lo señalo explícitamente y remito al archivo
> (ver la nota del §3 sobre `ROADMAP.md §3.4`).

---

## 1. Objetivo y motivación

### Qué se digitaliza
Las **localidades del Censo Nacional de Población 1960** de Argentina, a **nivel localidad**, con tres
piezas por fila:

- **localidad** (nombre tal como lo imprimió el censo, y su nombre oficial moderno),
- **población** (total 1960; y varones/mujeres en el crudo),
- **coordenadas** (lat/lon), más el **id INDEC** y el departamento moderno.

El punto clave del diseño es que se permiten **VARIOS puntos por departamento** (una fila por localidad),
**no** un único centro por departamento. Esto captura la **geografía económica intra-departamental**: en
1960 la población de un departamento no estaba en su centro geométrico, sino concentrada en pueblos,
estaciones y cabeceras.

### Por qué (enganche con Market Access)
Hoy el pipeline de MA ancla cada unidad espacial en **un solo centroide geométrico por departamento**.
En `code/pipeline/03c_compute_taus.R`, la función `load_centroids()` (línea 86) construye ese punto con:

```r
# code/pipeline/03c_compute_taus.R:110
cents_sf <- suppressWarnings(sf::st_centroid(shp))
```

es decir, `sf::st_centroid()` sobre los polígonos IPUMS `geolev2` — **un punto por polígono**, ubicado en
el centro geométrico del departamento, sin ponderar por dónde vivía la gente. Esos centroides alimentan
después `compute_one_case()` y el cálculo de distancias de costo (`gdistance::costDistance`) que produce
los `tau`.

El objetivo de este trabajo es **reemplazar o complementar** ese centroide único por los **puntos
múltiples ponderables por población** que salen del censo 1960, para que el market access se calcule desde
donde realmente estaba la actividad económica y no desde un centro geométrico arbitrario. El detalle del
enganche está en §5.

---

## 2. Método

El proceso está organizado en **pasos, y cada paso es un algoritmo determinístico** escrito *antes* de
ejecutarse (regla de planificación del proyecto: "planificar un paso = producir su procedimiento
casi-algorítmico con IF/THEN, casos, dudas y PROHIBIDOS, y dejarlo escrito en `README.md`"). Abajo cada
paso en resumen; el algoritmo completo vive en `README.md`.

### Reglas duras transversales (valen para todos los pasos)
Estas reglas son el corazón del método y explican por qué los resultados son auditables:

1. **Transcripción VERBATIM.** El Paso 1 copia lo impreso tal cual (acentos, abreviaturas, ortografía
   histórica, errores de imprenta). No se moderniza, no se expande, no se "corrige" nada durante la
   transcripción.
2. **El crudo es INMUTABLE.** `poblados_1960.csv` (la transcripción) nunca se toca. Ninguna corrección
   posterior lo modifica.
3. **`decisiones.csv` = log de TODA transformación.** Cualquier normalización, corrección o mapeo queda
   registrado como una fila en `decisiones.csv` (`paso, tipo, scope, page, row, campo, valor_original,
   valor_final, motivo, fuente`), reversible y trazable. **Nada cambia en silencio.**
4. **La capa automática solo PARTE, nunca decide.** En cada paso de chequeo/match, la máquina es una
   **función pura**: separa cada fila en `pasa-limpio` (superó un test exacto) o `flag` (no lo superó).
   **No corrige valores, no elige, no "auto-resuelve".** No existe el estado "resuelto por la máquina".
5. **El humano es el único punto de decisión.** Todo lo `flag` va a una cola de revisión humana. La
   **imagen impresa es la autoridad** sobre lo transcripto; Georef/INDEC sobre el nombre/coordenada
   moderna; la **investigación web con `fuente_url`** sobre los cambios históricos.
6. **"Sin fuente no hay coordenada."** Está PROHIBIDO inventar una lat/lon o un nombre. Toda coordenada
   tiene una fuente citada (registro Georef, o URL).
7. **Regla del CONJUNTO PERMITIDO** (clave para no aceptar homónimos en silencio). Cada
   `(provincia, departamento)` de 1960 se mapea, vía crosswalk, a un **conjunto** de departamentos
   modernos (identidad/rename → 1 elemento; split → {padre sobreviviente + hijos}). Un match de localidad
   solo se acepta si el departamento moderno del candidato **pertenece a ese conjunto**. Así los
   departamentos que se subdividieron dejan de generar falsos flags, pero un tocayo en un departamento
   lejano **sigue flageando** (el departamento es la única verificación independiente de que el match es
   el lugar correcto). Ver §4 (P1–P4) para por qué esta regla existe.

### Paso 1 — Transcripción verbatim por visión → `poblados_1960.csv`
Entrada: las mitades ampliadas de cada página escaneada (`pages/halves/v{Z}_p{NN}_h{1,2}.png`). Salida:
filas `page, n_orden, provincia, departamento, localidad, footnote, total, varones, mujeres`. Se lee cada
renglón de arriba hacia abajo, verbatim; se maneja la **costura** entre mitades (misma fila si comparten
`n_orden`); las celdas dudosas se **amplían con zoom** y, si sigue la duda, se anota en
`dudas_transcripcion.csv` (no se corrige — eso es Paso 2). Durante el Paso 1 está PROHIBIDO chequear
sumas, consultar Wikipedia, geocodificar o normalizar. Es transcripción y nada más.

### Paso 1B — Vocabulario canónico jerárquico → `autoridad_prov.csv`, `autoridad_depto.csv`
Fija la clave canónica `(provincia, departamento)` **dentro** del censo 1960, deduplicando variantes de
grafía (p. ej. `Cura-Co`/`Cura-Có` → una sola forma). Provincia = conjunto cerrado de 24 (provincias +
territorios de 1960; Tierra del Fuego era Territorio Nacional). Departamento = lista única **por
provincia** (por homónimos: hay "25 de Mayo" y "San Martín" en varias provincias). La máquina agrupa por
forma normalizada y por cercanía (Levenshtein ≤ 2) y **flaggea**; el humano fija el canónico. **1B NO
mapea a nombres modernos** (eso es Paso 3): construye el vocabulario vintage-1960.

### Paso 2 — Checks → `validacion_*.csv`, `cola_humana.csv`, ledgers
Capa automática 100% determinística que solo PARTE filas:
- **2.0 Dudas:** recorta la celda de cada duda del Paso 1 y la manda a revisión humana (no infiere).
- **2.1 Números:** `total == varones + mujeres`; si no cuadra → `flag` (no adivina qué celda falla).
- **2.2 Nombres (Wikipedia):** compara el nombre normalizado contra el título de Wikipedia ES (con
  contexto de departamento por homónimos). Exacto → `validado`; cualquier otra cosa (`aproximado`
  Levenshtein, o `sin_match`) → `flag`. **Wikipedia es sugerencia, no autoridad** (la oficial sale de
  Georef en el Paso 3).
- **2.4 Departamento (xlsx `1c1960_3_*`):** suma por departamento vs. planilla de control. **DIFERIDO**
  (los xlsx no están en el checkout; es una validación independiente que NO bloquea el Paso 3).
- **2.3 Revisión humana:** cola consolidada (`cola_humana.csv`) con recorte de imagen + crudo + evidencia.
  El humano decide leyendo la imagen. El estado "resuelto" no vive en la cola (que es *stateless* y
  recomputa los flags desde el crudo), sino en **ledgers append-only** (`decisiones.csv`,
  `revision_aproximados.csv`, `dudas_resueltas.csv`). La cola se calcula como **`flags − ledgers`** y a
  cada ítem le pone `estado` (`open`/`resuelto`) + `resuelto_por`. Los nombres `sin_match`/`sin_sugerencia`
  (sin artículo en Wikipedia = sin señal de typo) se **difieren en masa al Paso 3** vía
  `nombres_diferidos_paso3.csv` (Georef los canoniza), sin revisión individual.

### Paso 3 — Georreferenciación contra Georef/INDEC
Motor: `scripts/geocode_georef.py` + la API pública de Georef (`apis.datos.gob.ar/georef/api`), con caché
de firma completa en `georef_cache.json` (clave = `capa|nombre|provincia|departamento|exacto|max`), de
modo que el caché sea **algoritmo-independiente** y reproducible. Sub-pasos:

- **3.0 Provincias** → homogeneizar las 23 provincias/territorios a los nombres/ids de Georef. Único caso
  no trivial: Tierra del Fuego (confirmación humana única). Verificado que no haya filas CABA en el crudo.
- **3.1 Crosswalk de departamentos 1960 → INDEC** → `crosswalk_indec.csv`. Auto-match por nombre
  (identidad); todo lo demás → `flag` resuelto por José con investigación web (`rename` / `split` /
  `merge` / `sin_equivalente`), **cotejado con `code/base/census_1960/clean_census_1960.R`
  (`apply_name_changes()`)** del repo. PROHIBIDO asignar un departamento moderno no-identidad sin
  `fuente_url`.
- **3.1b Historia territorial de departamentos** → filas `split`. Existe porque 3.1 testea *persistencia
  de nombre*, no *persistencia territorial*: un departamento puede conservar el nombre y subdividirse
  (ver §4 P1). Por provincia se enumeran los departamentos modernos creados **post-1960** y su padre 1960
  (fuente: anexos de Wikipedia con ley/año, leyes provinciales, INDEC, y el **codebook CELADE 1960**
  `ref/ar60divp.pdf` como fuente primaria de la división de 1960). Cada hijo post-1960 → fila `split` con
  `fuente_url`. El resultado define el **conjunto permitido** de cada `(prov, depto)` 1960.
- **3.2 Match de localidad** → `geo_match_simple.csv`. Para cada localidad se resuelve su conjunto
  permitido y se consulta Georef `localidades-censales` filtrando por ese conjunto. **La igualdad de
  nombres la decide NUESTRO código con `norm_name()`, NO la API** (ver §4 P6). Seis desenlaces:
  `auto_ok` (1 hit exacto in-set) · `flag_ambiguo` (>1 in-set) · `flag_depto` (nombre exacto en la
  provincia pero fuera del conjunto → transferencia de límite o homónimo) · `flag_bahra` (no es censal;
  aparece en la capa BAHRA `localidades`/`asentamientos`) · `flag_variante` (fuzzy in-set en censal) ·
  `flag_variante_bahra` (fuzzy in-set en BAHRA). Todo lo `flag_*` → 3.3a/3.3.
- **3.3a Investigación documentada de los flags** → `investigacion_flags.csv` (+ `c5_investigacion.csv`
  para los "nombres sueltos" C5). Cada flag recibe una **propuesta con `fuente_url` + confianza**,
  ordenada por población 1960 (concentra la atención donde pesa el MA). **3.3a solo PROPONE; no
  auto-confirma.** Los C5 pasan además por un **guard geográfico determinístico**: cada coordenada
  propuesta (venga de Georef, Wikipedia u OSM) se reverse-geocodea con Georef `/ubicacion` → el
  departamento del punto debe caer en el conjunto permitido (`verificado_geo = verde`); si no, `rojo`
  (escrutinio humano); si cae en mar/exterior, la **coordenada se rechaza**.
- **3.3 Confirmación humana** → **EN CURSO** (ver §3 y §6). José confirma/corrige cada flag leyendo
  imagen + fuente; cada decisión → `decisiones.csv`. La cola se dividió en **Grupo A** (52 casos
  uno-por-uno, incl. los 37 `flag_depto`), **Grupo B** (172 en lote sobre el mapa) y **Grupo C** (17
  sin_coordenada). El **Grupo A ya está CERRADO** (ver 3.3b abajo); B y C siguen pendientes.
- **3.3b Cierre del Grupo A vía pasada web documentada** (decisión de José: resolver los `flag_depto` con
  investigación web en vez de revisión manual, porque nunca habían tenido web — sólo el `dist_km`). Se
  extendió a los `flag_depto` el protocolo web de C5: cascada Georef→Wikipedia→OSM→gaceteros + **guard
  geográfico**, en dos pasadas. La 2ª pasada (**3.3b-2**) exige **documentar el acto administrativo** del
  cambio de departamento (ley/decreto), no aceptar por unicidad de nombre; y un **chequeo de contigüidad**
  geográfica (borde compartido) como evidencia independiente. Cada aceptación → `decisiones.csv`
  (paso=3.3b). Detalle numérico en §3.6a.
- **3.4 Ensamble** → `coordenadas_1960.csv` (una fila por localidad; varias por departamento; las sin
  coordenada quedan con lat/lon vacío + motivo). QC visual en `mapa_qc.html` (SVG offline autocontenido).

---

## 3. Resultados (números verificados en vivo, 2026-07-22)

> **Recordatorio de estado: PROVISIONAL (con avance).** Los resultados de georreferenciación (Paso 3)
> están armados; el **Grupo A** de la confirmación humana ya está **cerrado** (50 casos vía la pasada web
> documentada 3.3b + 2 del PASO 0 = 52), pero **faltan el Grupo B (172), el Grupo C (17) y la validación
> por muestra del bloque `auto_muestreo` (829)**. Los conteos de abajo describen el ensamble
> `coordenadas_1960.csv` tal como está hoy.

### 3.1 Cobertura de transcripción (Paso 1 — COMPLETO)
`poblados_1960.csv` (**crudo, inmutable**) = **3.063 localidades**, todo el país, zonas 3–9:

| Zona | Provincias | Localidades |
|---|---|---|
| 3 | Buenos Aires + La Pampa | 909 |
| 4 | Córdoba + Santa Fe | 775 |
| 5 | Corrientes + Entre Ríos + Misiones | 304 |
| 6 | Chaco + Formosa + Santiago del Estero | 285 |
| 7 | Jujuy + Salta + Tucumán + Catamarca + La Rioja (NOA) | 430 |
| 8 | Mendoza + San Juan + San Luis (Cuyo) | 200 |
| 9 | Río Negro + Chubut + Neuquén + Santa Cruz + Tierra del Fuego (Patagonia) | 160 |
| **Total** | | **3.063** |

### 3.2 Cierre del Paso 2 (checks — CERRADO salvo 2.4)
`cola_humana.csv` = **742 ítems, todos `resuelto`, 0 `open`** (verificado). Ledgers de cierre:
`decisiones.csv` (del Paso 2: 48 del paso 2.3 + 16 del 1B; hoy el archivo tiene **116** entradas en total
porque sumó 2 del PASO 0/3.3 + 50 del 3.3b, ver §3.6a),
`revision_aproximados.csv` (veredicto por-ítem de los `aproximado`, país completo), `dudas_resueltas.csv`.
La regla de defer materializó **529** nombres `sin_match`/`sin_sugerencia` → `nombres_diferidos_paso3.csv`.
Único pendiente del Paso 2: **2.4 depto-xlsx** (espera los `1c1960_3_*`; no bloquea el Paso 3).

### 3.3 Match del Paso 3.2 (`geo_match_simple.csv`) y su evolución
Distribución **actual (v4)** de los 3.063:

| estado 3.2 | n | % |
|---|---:|---:|
| `auto_ok` (match exacto en 3 campos, in-set) | **1993** | 65,1% |
| `flag_bahra` | 522 | 17,0% |
| `flag_sin_match` | 250 | 8,2% |
| `flag_variante` | 192 | 6,3% |
| `flag_variante_bahra` | 68 | 2,2% |
| `flag_depto` | 37 | 1,2% |
| `flag_ambiguo` | 1 | 0,03% |
| **Total flags** | **1070** | **34,9%** |

**Evolución v1→v4** (cada transición está explicada y verificada en `PROBLEMAS_paso3.md`; ninguna alteró
un `georef_id` ya asignado a un `auto_ok`):

| estado | v1 | v2 (conjunto permitido) | v3 (fix U+00AD) | v4 (tier BAHRA-fuzzy) |
|---|---:|---:|---:|---:|
| `auto_ok` | 1915 | 1956 | 1993 | 1993 |
| `flag_bahra` | 530 | 543 | 522 | 522 |
| `flag_sin_match` | 341 | 321 | 318 | 250 |
| `flag_variante` | 200 | 207 | 192 | 192 |
| `flag_variante_bahra` | — | — | — | 68 |
| `flag_depto` | 76 | 35 | 37 | 37 |
| `flag_ambiguo` | 1 | 1 | 1 | 1 |

- **v1→v2:** modelar los splits (3.1b) subió `auto_ok` +41 y bajó `flag_depto` −41 (los 41 splits
  documentados dejaron de ser falsos flags).
- **v2→v3:** el fix del carácter invisible U+00AD (P6) recuperó +37 `auto_ok` (22 por el soft hyphen,
  15 por guión↔espacio tipo `Bell-Ville`=`Bell Ville`).
- **v3→v4:** el nuevo tier `flag_variante_bahra` (P8) movió 68 `sin_match` a "propuesta para revisar".

### 3.4 Crosswalk INDEC (`crosswalk_indec.csv`)
- **529 filas** para **487** pares `(provincia, departamento)` 1960 únicos.
- **32** departamentos con **conjunto múltiple** (splits: 1 depto 1960 → varios modernos).
- Por tipo: **identidad 462 · split 38 · rename 19 · split_disuelto 6 · especial 2 · sin_equivalente 2.**
- 0 filas no-identidad sin `fuente_url`; determinístico (doble corrida idéntica).

### 3.5 Cobertura de la investigación 3.3a (`investigacion_flags.csv`)
Los **1070 flags** con propuesta + fuente, ordenados por población 1960:

| tier | n | qué es |
|---|---:|---|
| A (aparcado) | 696 | bahra/variante: el registro Georef ES la evidencia (no se revisó en detalle todavía) |
| C5-manual_coord | 89 | coord citada de web (Wikipedia/OSM/gacetero) |
| C5-rename | 71 | cambio de nombre resuelto contra Georef |
| E-bahra-fuzzy | 67 | fuzzy laxo en BAHRA — **confianza BAJA** (confirmar o rechazar) |
| C-generador | 48 | candidato generado por transformación (prefijo/abreviatura/nº-palabra/paréntesis) con `rationale` |
| B | 37 | transferencia de límite vs homónimo (evidencia = `dist_km` entre centroides) |
| D | 19 | ambigüedad (>1 candidato) |
| C5-sin_coordenada | 15 | documentado "no hallado" |
| C5-barrio_de | 13 | barrio/paraje de una localidad mayor |
| C1-cabecera | 11 | el censo listó la cabecera bajo el nombre del depto; con `fuente_url` |
| C-especial | 2 | `manual_coord` citada (casos especiales) |
| C5-pendiente | 2 | bloqueados por artefacto (hoy destrabados, ver §3.7) |

**Población flageada (filas que no son `auto_match`) = 1.748.097 hab.** La gran mayoría ya tiene propuesta
con fuente; los que no la tienen por diseño (B, D) llevan **evidencia** (distancia/candidatos), no un
candidato único.

**Investigación C5 (`c5_investigacion.csv`) = 188 casos:** verde 161 · rojo 10 · sin_depto 2 · sin_coord 15
→ **173 con coordenada**. El guard geográfico cazó, entre otros, el homónimo de `Zonda` y un error de
coordenada de Wikipedia en `Dixonville`. **0 coords sin `fuente_url`.**

### 3.6 Ensamble 3.4 (`coordenadas_1960.csv`) — PROVISIONAL
**3.063 filas** (una por localidad). Estado de coordenada:

| | n | población |
|---|---:|---:|
| **Con coordenada** | **3.046** | 13.510.177 hab ubicados |
| Sin coordenada | 17 | 34.509 hab |

Por `estado`: `auto_ok` **1993** · `propuesto` **1003** · `confirmado_3.3b` **50** · `sin_coordenada` **17**.
(`propuesto` bajó de 1053 a 1003 y aparecieron los 50 `confirmado_3.3b` al cerrar el Grupo A.)

### 3.6a Cierre del Grupo A (Paso 3.3b) — HECHO
Los **52** casos del Grupo A (los 37 `flag_depto` + casos especiales) quedaron **aceptados**:

- **50** resueltos por la pasada web documentada 3.3b (**31 auto-documentados** con `fuente_url` + guard
  verde/transferencia, **19 ratificados por José** en `veredictos_3.3b.csv`) → pasan a estado
  `confirmado_3.3b` (76.000 hab).
- **2** del PASO 0 (`La Larga`, `Puerto Madryn`), corregidos contra imagen + fuente primaria.

La 2ª pasada (3.3b-2) documentó el acto administrativo de cada transferencia: **6 `documentada`** (p. ej.
Quequén, Decreto-Ley 9327/1979; Yerba Buena / Bella Vista / Río Colorado / El Manantial, Ley Tucumán
4518/1976; Gob. Racedo, Ley ER 6378/1979), **1 artefacto** (Santa Teresa: la fuente 1960 imprimió depto
`Iriondo` erróneo → correcto **Constitución**, confirmado por imagen), y **14 `limite_no_documentada`**
(reasignaciones entre departamentos preexistentes = el límite declarado de §3.1b). El **chequeo de
contigüidad** (`check_contiguidad.py` sobre `ref/deptos_argentina.geojson`) dio **14/14 contiguos** — borde
compartido, evidencia geográfica independiente de la unicidad de nombre. **0 residuo, 0 coord sin
`fuente_url`, determinístico.** Volcado: `revision_3.3.csv` Grupo A = 52 `aceptar`; `decisiones.csv` +50
(paso 3.3b). Archivos nuevos: `veredictos_3.3b.csv`, `transfer_doc.csv`, `contiguidad_transfer.csv`,
`revision_3.3_web.csv`; scripts `build_web33_input.py`, `build_revision_web.py`, `build_transfer_doc.py`,
`check_contiguidad.py`, `build_volcado_3.3b.py`.

### 3.7 QC del guard geográfico (`verificado_geo`)
Sobre las 3.063 filas del ensamble (**tras el cierre del Grupo A**):

| `verificado_geo` | n | qué significa |
|---|---:|---|
| verde | **3023** | la coord cae en el conjunto de departamentos esperado |
| rojo | **23** | cae fuera del conjunto, pero **todas explicadas** (ya no son "a revisar") |
| (vacío) | 17 | sin coordenada |

Los `rojo` bajaron de 47 a **23** al cerrar el Grupo A: los homónimos se resolvieron (→ verde) y los 23
que quedan son **transferencias documentadas + contiguas**, todas con `fuente_url`: 14
`transferencia_limite_declarado` + 5 `transferencia_confirmada` + 3 `manual_coord_frontera` + 1
`correccion_fuente_depto`. Población en filas `rojo` = **33.098 hab**. (Ya no hay `especial`/`revisar_ambiguo`
sueltos: se resolvieron en 3.3b.)

### 3.8 Tabla de estados de aceptación (para la confirmación 3.3)
El ensamble distingue **cómo se aceptaría** cada coordenada (columna `criterio_aceptacion`), para que la
confirmación humana no sea un default silencioso:

| `criterio_aceptacion` | n | qué es |
|---|---:|---|
| `auto_match` | 1993 | match exacto en 3 campos ("ya venían" bien) |
| `auto_muestreo` | 829 | flags a auto-aceptar validando una **muestra** (no uno por uno) — **pendiente Q2** |
| `humano_lote` | 172 | Grupo B: se revisan en bloque sobre el mapa — **pendiente** |
| `documentado_3.3b` | 31 | Grupo A: auto-documentado en 3.3b (web + guard) — **cerrado** |
| `humano_individual` | 21 | Grupo A: uno por uno con evidencia (19 de 3.3b + 2 del PASO 0) — **cerrado** |
| `sin_coordenada` | 17 | Grupo C: quedan sin lat/lon, con motivo — **pendiente confirmar** |

`decisiones.csv` = **116 entradas** (paso 3.3b **50** + 2.3 48 + 1B 16 + 3.3 2). El Grupo A (52 =
31 `documentado_3.3b` + 21 `humano_individual`) está aceptado; cada aceptación quedó volcada como decisión
(nada en silencio).

> **⚠ Discrepancia documentada (regla "mandá el archivo").** El bloque **"Paso 3.4"** de `ROADMAP.md`
> (≈líneas 237–245) sigue diciendo "**3.044** con coordenada · **2 bloqueado_artefacto**" y "verde
> **2994** · rojo 47 · **ambar 3**", y la línea de auditoría del bloque "Paso 3.3 PREPARADO" (≈l.255)
> lista `humano_individual` **52**. **Todo eso quedó superado por el cierre del Grupo A (3.3b).** Los CSV
> vivos dan: **3.046** con coordenada, **0** bloqueado_artefacto, `verificado_geo` **verde 3023 / rojo 23**
> (sin `especial`/`ambar`), y `criterio_aceptacion` **documentado_3.3b 31 + humano_individual 21**. Los 2
> `bloqueado_artefacto` se destrabaron en el PASO 0 (**Puerto Madryn** → Chubut/**Biedma** [26007020];
> **La Larga** → Buenos Aires/**Daireaux** [06231040]). El bloque más nuevo del `ROADMAP.md` (el cierre
> 3.3b, ≈l.285–289) sí coincide con estos números. **Este informe usa los números vivos.**

---

## 4. Aprendizajes / problemas de método (P1–P8)

`PROBLEMAS_paso3.md` es un log honesto de los baches del Paso 3. Los detallo uno por uno porque cada uno
dejó una regla de método.

- **P1 — Splits con padre sobreviviente no modelados.** El crosswalk 3.1 testeaba *persistencia de
  NOMBRE*, no *persistencia TERRITORIAL*: marcaba `identidad` a todo departamento 1960 cuyo nombre
  sobrevive. Pero un departamento puede **conservar el nombre y subdividirse** (ceder territorio a un hijo
  nuevo). Esos splits quedaban invisibles al name-match. **Medición del punto ciego: de 46 padres
  realmente subdivididos, 42 estaban marcados `identidad`.** → Se corrigió modelando la historia
  territorial **por adelantado y sistemáticamente** en 3.1b.

- **P2 — `flag_depto` mezclaba tres cosas distintas.** Sin historia ni geografía, "el nombre existe exacto
  en otro departamento" no distingue un **hijo-de-split** de una **transferencia de límite** de un
  **homónimo lejano** (p. ej. `Pavón`: Exaltación de la Cruz → General Lavalle, ~300 km). → 3.1b acota lo
  documentable; el residuo se ordena con un **discriminador de distancia entre centroides** (evidencia
  dura que ordena la cola, no decide): <60 km = transferencia plausible; ≥150 km = homónimo probable.

- **P3 — Un typo humano en la resolución manual de 3.1, cazado por el propio loop.** En La Rioja se había
  escrito "General Ángel Vicente Peñaloza" para `Vélez Sarsfield`, pero Georef lo llama **"Ángel Vicente
  Peñaloza" [46056]**. El error **lo destapó el loop `flag_depto`** (esas 2 localidades cayeron en flag) —
  un beneficio del método: un check que puede contradecir la tabla que uno mismo construyó. Corregido en
  la reconstrucción del crosswalk.

- **P4 — Riesgo residual de `auto_ok` errado por homónimo + split.** Si una localidad migró al hijo y
  queda un tocayo con el mismo nombre en el padre, el match exacto en el padre daría el lugar equivocado.
  Riesgo bajo, mitigado al modelar los splits (3.1b). Se documenta, no se oculta.

- **P5 — `flag_sin_match` es un problema de NOMBRE de localidad, ortogonal a la historia de departamentos.**
  (Prefijos "Balneario/Colonia/Villa", grafía, o ausencia de la capa censal.) Se resuelve en 3.3a (2ª
  pasada Georef + investigación web con fuente), no tocando el crosswalk.

- **P6 — El carácter invisible U+00AD de Georef mandaba Junín y Olavarría a BAHRA, en silencio.**
  *Síntoma:* José preguntó "los flag_bahra, ¿por qué estarían mal?" y saltó lo absurdo: **Junín (53.489
  hab) y Olavarría (35.107 hab)** son ciudades grandes, no pueden ser parajes de BAHRA. *Causa raíz:* los
  datos de Georef traen un **U+00AD (SOFT HYPHEN, invisible)** incrustado después de la `í` (bytes reales
  de "Junín": `J u n í [U+00AD] n`). Para el servidor `"Junín" != "Juní­n"`, así que `exacto=true`
  devolvía 0 hits y la localidad caía a BAHRA o a `sin_match`. **Alcance medido: 18 localidades mal
  clasificadas, 125.463 habitantes.** *Causa raíz REAL (arquitectural):* delegábamos la **igualdad de
  nombres** a `exacto=true`, es decir a la comparación de strings del servidor sobre datos sucios que no
  controlamos. *Arreglo (general, no un parche):* `norm_name()` en `geocode_georef.py` como fuente única
  de verdad, que quita **toda la categoría Unicode `Cf`** (cubre soft hyphen, zero-width, BOM — genérico,
  no lista negra); **PROHIBIDO usar `exacto=true` como criterio** (se consulta sin `exacto` y la igualdad
  la decide nuestro código); la comparación de departamento también normalizada. Verificado con
  `scripts/test_norm_name.py` (14 tests + 12 casos reales + test de no-colisión: `Junín`≠`Junín (Est.)`).

- **P7 — Al verificar los `flag_bahra`, aparecieron dos bugs más de Georef.**
  - **P7.1 — Ids INDEC desalineados ENTRE CAPAS.** En el depto Necochea, el mismo nombre con la misma
    coordenada tiene **id distinto** en la capa `localidades` vs `localidades-censales` (la capa
    `localidades` no lista `Energía` y desde ahí **corre todos los ids un lugar**). Conclusión: el pareo
    **nombre↔coordenada es correcto en ambas capas** (nuestras coordenadas son confiables), pero el
    `georef_id` de los **77** `flag_bahra` que vienen de la capa `localidades` **no es un id censal INDEC
    válido** → se marcan `id_no_censal=true`. Los `auto_ok` **no** están afectados (salen solo de la capa
    censal). Además quedó claro que `flag_bahra` **no es un error**: post-fix significa "el nombre no es
    una localidad censal" (caso `Necochea`: INDEC fusionó en `Necochea - Quequén`, BAHRA conserva
    `Necochea` sola → para el mapeo 1960 **BAHRA es la fuente MEJOR**).
  - **P7.2 — `campos=completo` devuelve mojibake doble-encodeado** (`"PehuajÃ³"`). Con campos específicos
    no pasa → **PROHIBIDO usar `campos=completo`**.

- **P8 — Asimetría censal/BAHRA en 3.2: faltaba el tier BAHRA-fuzzy.** *Disparador:* José pidió "estudiá
  las de C5 a ver si hay algo sistemático y determinístico" **antes** de mandar 266 flags a investigación
  manual. Al hacerlo apareció un hueco: 3.2 trataba las capas de forma asimétrica — censal tenía
  exacto **y** fuzzy, pero BAHRA **solo exacto**. Un paraje presente en BAHRA con el nombre apenas distinto
  caía a `sin_match` **sin que nadie lo mirara** (68 casos). → Nuevo tier `flag_variante_bahra`, después
  del censal-fuzzy. *Advertencia registrada:* el fuzzy es **laxo** (confianza baja); "no resuelve" los 68
  casos, los mueve de "sin propuesta" a "propuesta para confirmar o rechazar".

### Lecciones generales (para no repetirlas)
1. **"El nombre sobrevive" ≠ "la unidad no cambió".** Al reconciliar unidades administrativas entre dos
   épocas hay que modelar la **historia territorial**, no solo comparar strings.
2. **Diseñá siempre un check que pueda contradecir tu propia tabla.** El loop `flag_depto` destapó los 42
   splits ocultos **y** un typo humano (P3).
3. **Estrictez = seguridad.** Aflojar un filtro para reducir flags cambia **trabajo** por **error
   silencioso**. En datos de investigación se prefiere el flag. (Por eso se descartó el "department-last".)
4. **Lo no documentado se declara como límite, no se disimula.** Las transferencias de límite entre
   departamentos preexistentes no figuran en los anexos → quedan como residuo caso-a-caso explícito.
5. **No delegues una decisión propia a la comparación de un tercero sobre datos que no controlás** (P6).
   La igualdad de nombres es una decisión del método ⇒ vive en nuestro código, documentada y testeada.
6. **Un resultado absurdo vale más que cien plausibles.** El bug P6 no lo encontró ningún test: lo
   encontró notar que "Junín y Olavarría no pueden ser parajes de BAHRA".
7. **Auditar antes de parchear.** Ante "hay un problema con caracteres especiales", la respuesta fue
   auditar el universo entero (Georef, canon, crosswalk) y recién ahí fijar un normalizador genérico.

---

## 5. Salida y enganche al pipeline

### 5.1 Esquema de `coordenadas_1960.csv` (la salida final)
Una fila por localidad 1960 (varias por departamento; **no se colapsa** a un único punto). 24 columnas:

| # | columna | qué es |
|---:|---|---|
| 1 | `page` | página del censo (clave de origen, junto con `n_orden` + `localidad_canon`) |
| 2 | `n_orden` | nº de orden impreso en esa página |
| 3 | `provincia_canon` | provincia canónica 1960 (Paso 1B) |
| 4 | `departamento_canon` | departamento canónico 1960 (Paso 1B) |
| 5 | `localidad_canon` | localidad canónica 1960 (crudo + `decisiones.csv` aplicadas) |
| 6 | `footnote` | marcador de nota al pie (p. ej. `1` = total del partido del conurbano) |
| 7 | `total` | **población total 1960** de la localidad (el peso para el MA) |
| 8 | `georef_id` | id INDEC/Georef del match (5 díg. si es censal; ver P7.1 sobre `id_no_censal`) |
| 9 | `nombre_oficial` | nombre oficial moderno (Georef) |
| 10 | `georef_depto` | departamento moderno del match |
| 11 | `lat` | latitud (vacío si `sin_coordenada`) |
| 12 | `lon` | longitud (vacío si `sin_coordenada`) |
| 13 | `estado` | `auto_ok` \| `propuesto` \| `confirmado_3.3b` \| `sin_coordenada` |
| 14 | `tier` | tier de resolución (`auto_ok`, `A (aparcado)`, `C1-cabecera`, `C5-*`, `B`, `D`, `PASO0-correccion`, `3.3b:transferencia_limite_declarado`, `3.3b:transferencia_confirmada`, `3.3b:manual_coord_frontera`, `3.3b:correccion_fuente_depto`…) |
| 15 | `criterio_deteccion` | cómo se detectó/resolvió (`match_exacto_3campos`, `bahra_georef`, `web_manual_coord`, `cabecera_documentada`, `depto_transferencia_o_homonimo`…) |
| 16 | `criterio_aceptacion` | cómo se aceptaría/aceptó (`auto_match` \| `auto_muestreo` \| `humano_lote` \| `documentado_3.3b` \| `humano_individual` \| `sin_coordenada`) — ver §3.8 |
| 17 | `en_muestra` | si la fila entró en la muestra de validación del bloque `auto_muestreo` |
| 18 | `decidido_por` | quién/qué la aceptó (`auto`, `auto(pendiente_muestra)`, `pendiente:Jose`, `auto:3.3b(web+doc+guard)`, `humano:Jose(3.3b)`, `humano:Jose(Paso0)`) |
| 19 | `fecha` | fecha de la decisión |
| 20 | `confianza` | `alta` / media / baja (los `E-bahra-fuzzy` son baja) |
| 21 | `fuente` | fuente de la coordenada (`georef/localidades-censales`, `web+georef`, …) |
| 22 | `fuente_url` | URL citada (regla "sin fuente no hay coordenada") |
| 23 | `verificado_geo` | resultado del guard geográfico (hoy: `verde`/`rojo`/vacío; los `especial`/`revisar_ambiguo` previos se resolvieron en 3.3b) |
| 24 | `nota` | nota libre trazable (motivo, aclaraciones, homónimos descartados) |

**Trazabilidad:** cada coordenada tiene `estado` + `tier` + `fuente`/`fuente_url` + `verificado_geo`, y es
reproducible desde el crudo + `georef_cache.json` + los ledgers (`decisiones.csv`, etc.).

### 5.2 Cómo se engancha al pipeline de Market Access
Hoy (`code/pipeline/03c_compute_taus.R`, `load_centroids()` → `sf::st_centroid(shp)` en la **línea 110**)
el pipeline usa **un centroide geométrico por `geolev2`**. La integración consiste en **reemplazar o
complementar** ese punto único por los **puntos múltiples 1960**:

- **Reemplazo simple (un punto por depto, mejor ubicado):** en vez del centroide geométrico, usar el
  **centroide ponderado por población 1960** de las localidades de cada `geolev2` (o la localidad más
  poblada como proxy de cabecera). Cambio localizado en `load_centroids()`.
- **Multi-punto (lo que el trabajo habilita):** dejar que cada `geolev2` tenga **varias localidades** con
  su población, y que el cálculo de accesibilidad/`tau` opere sobre esos puntos (agregando por origen).
  Requiere adaptar `compute_one_case()` para múltiples orígenes por unidad, no solo `costDistance` entre
  centroides.

El **join** entre este trabajo y el pipeline es por **`georef_id` / departamento INDEC** contra la clave
`geolev2` que usa el pipeline (`sub("^0+", "", GEOLEVEL2)`), usando el `crosswalk_indec.csv` como puente
1960→INDEC moderno. La referencia de limpieza existente en el repo es
`code/base/census_1960/clean_census_1960.R` (`apply_name_changes()`), que ya se cotejó al construir el
crosswalk.

### 5.3 Qué es salida final y qué es intermedio

| rol | archivos |
|---|---|
| **Salida final** | `coordenadas_1960.csv` (el producto), `mapa_qc.html` (QC visual) |
| **Crudo inmutable** | `poblados_1960.csv` |
| **Log de decisiones** | `decisiones.csv` (+ `revision_aproximados.csv`, `dudas_resueltas.csv`, `veredictos_3.3b.csv`) |
| **Intermedios trazables** | `crosswalk_indec.csv`, `geo_match_simple.csv`, `investigacion_flags.csv`, `c5_investigacion.csv`, `revision_3.3.csv`, `revision_3.3_web.csv`, `transfer_doc.csv`, `contiguidad_transfer.csv`, `autoridad_prov.csv`, `autoridad_depto.csv`, `vista_ancha.csv` |
| **Caché reproducible** | `georef_cache.json`, `wiki_cache.json`, `ubicacion_cache.json` |
| **Insumo (imágenes)** | `pages/` (607 MB, 1240 PNG; `pages/halves/` es regenerable) |
| **Código** | `scripts/` (≈30 scripts: `geocode_georef.py`, `build_crosswalk_indec.py`, `build_geo_match.py`, `build_investigacion_flags.py`, `build_c5.py`, `guard_ubicacion.py`, `build_coordenadas.py`, `build_mapa_qc.py`, `test_norm_name.py`, …) |

---

## 6. Pendientes

1. **Paso 3.3 — confirmación humana (lo que falta para dejar de ser PROVISIONAL).**
   - **Grupo A (52) — HECHO** (cerrado vía 3.3b, ver §3.6a).
   - **Grupo B (172, `humano_lote`) — PENDIENTE:** revisión en bloque por vistazo al `mapa_qc.html`;
     `veredicto` a completar en `revision_3.3.csv` (hoy 172 vacíos).
   - **Grupo C (17, `sin_coordenada`) — PENDIENTE:** confirmar que quedan sin coordenada (documentado).
   - **Muestra Q2 del bloque `auto_muestreo` (829) — PENDIENTE:** `muestra_paso2.csv` (**30** filas,
     semilla fija) para aceptar el bloque por muestreo en vez de uno por uno; de esa decisión depende el
     flip `auto_muestreo` → confirmado + marcar `en_muestra`. Tier A (696) entra como `auto_muestreo`
     salvo que José lo revise.
   - Al cerrar B/C/Q2 se **re-ensambla** `coordenadas_1960.csv` definitiva y se vuelca **cada aceptación**
     a `decisiones.csv` (append-only; nada en silencio), igual que se hizo con el Grupo A.

2. **Paso 2.4 — validación depto-xlsx.** Espera los archivos `1c1960_3_*` (suma de población por
   departamento). Es una validación **independiente** del match; **no bloquea** el Paso 3.

3. **Estaciones de ferrocarril (proceso B) — DIFERIDO.** Es un **insumo separado**, para el canal
   ferroviario tipo Gibbons; no forma parte de este entregable de localidades. Se hará en otra sesión.

---

## 7. Cómo integrar esto al repo — preguntas para Diego

**Contexto (para que decidas con la info completa).** Trabajé todo esto **en local**: la carpeta
`_local_geocoding_1960/` está **gitignored** y mantuve el repo en **solo-lectura** (regla del proyecto).
Dato técnico: la carpeta hoy no está ignorada por el `.gitignore` trackeado, sino por el archivo de
exclude **local** `.git/info/exclude` (línea 10) — así que para incorporarla hay que decidir cómo
"des-ignorarla". Puedo hacer un commit al repo para pasarte esto, pero **no quiero romper la estructura ni
las convenciones de replicabilidad (AER)**, así que prefiero que me digas exactamente cómo. Preguntas
concretas:

1. **¿Flujo?** ¿Alcanza con un **commit + push directo**, o querés que abra un **branch + PR**? Si es
   branch/PR, ¿a qué rama base (¿`main`?) y con qué nombre de branch?

2. **¿Qué archivos commiteo?** Opciones (no excluyentes):
   - solo la **salida final** `coordenadas_1960.csv` (896 KB);
   - también el **crudo** `poblados_1960.csv` + `decisiones.csv` (para trazabilidad);
   - también los **intermedios** (`crosswalk_indec.csv`, `geo_match_simple.csv`, `investigacion_flags.csv`,
     `c5_investigacion.csv`, `autoridad_*`, `vista_ancha.csv`);
   - también los **scripts/** (≈30 scripts Python del pipeline reproducible);
   - la **documentación** (`README.md`, `ROADMAP.md`, `PROBLEMAS_paso3.md`) + este informe;
   - la **carpeta entera**.
   Ojo con el **peso**: `pages/` = **607 MB / 1240 PNG** y `georef_cache.json` = **2,8 MB**. ¿Los
   incluimos, los dejamos afuera (regenerables / demasiado pesados para git), o van por Git LFS / release
   aparte?

3. **¿Dónde deberían vivir en el árbol del repo?** ¿En `data/derived/…` (p. ej.
   `data/derived/base/census_1960/`, que ya existe)? ¿En `code/base/…` (junto a
   `code/base/census_1960/clean_census_1960.R`)? ¿Una **carpeta nueva** dedicada? Tené en cuenta que
   `.gitignore` ignora `data/derived/**` salvo excepciones que conservan manifests, y que la convención
   del repo es commitear tablas como CSV y figuras como PNG.

4. **¿Hay que des-ignorar la carpeta**, o preferís que copie los archivos elegidos a la ubicación
   canónica del árbol (dejando `_local_geocoding_1960/` como está)? Si des-ignoramos, ¿editamos
   `.gitignore` o el exclude local?

Decime el detalle y armo el commit exactamente así, sin improvisar sobre la estructura.

---

### Anexo — referencias rápidas de archivos
- Protocolo/algoritmo completo: `_local_geocoding_1960/README.md`
- Plan / estado / cadencia: `_local_geocoding_1960/ROADMAP.md`
- Problemas y lecciones (P1–P8): `_local_geocoding_1960/PROBLEMAS_paso3.md`
- Salida final: `_local_geocoding_1960/coordenadas_1960.csv` · QC: `_local_geocoding_1960/mapa_qc.html`
- Hook del pipeline MA: `code/pipeline/03c_compute_taus.R` (`load_centroids()` l.86; `st_centroid` l.110)
- Referencia de limpieza en repo: `code/base/census_1960/clean_census_1960.R` (`apply_name_changes()`)
