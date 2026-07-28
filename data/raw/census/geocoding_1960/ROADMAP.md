# ROADMAP — Poblados 1960 (plan / estado / cadencia)

> Este archivo = el **PLAN** (qué, en qué orden, estado). El **PROTOCOLO** (cómo transcribir, reglas,
> esquema) vive en `README.md`. Los dos juntos hacen el directorio autocontenido.
> (Hay una copia de trabajo en `C:\Users\josem\.claude\plans\…` que usa Claude Code en plan mode;
> la versión canónica y autocontenida es ESTA.)

## Contexto / objetivo
Digitalizar y geocodificar las **localidades del censo 1960 a nivel localidad**
(localidad · población · coordenadas), permitiendo **VARIOS puntos por departamento** (NO un único
centro), para representar la **geografía económica intra-departamental** y anclar el Market Access
más fino que el centroide único actual (`sf::st_centroid`, `code/pipeline/03c_compute_taus.R:110`).
Regla: repo solo lectura; todo output acá (gitignored, no se commitea).

## Modo de trabajo (ACORDADO)
- **Incremental:** avanzar un paso → evaluar → planificar el siguiente EN DETALLE (plan mode) →
  ejecutar → repetir.
- **CADA PASO = UN ALGORITMO:** planificar un paso (1B, 2, 3) significa producir su **procedimiento
  casi-algorítmico** (pasos numerados, IF/THEN, manejo de casos/dudas, PROHIBIDOS), con el mismo
  nivel de detalle que el ALGORITMO del Paso 1, y dejarlo escrito en el `README.md` ANTES de ejecutar.
- **Trazabilidad (regla dura):** se transcribe VERBATIM; el crudo (`poblados_1960.csv`) es inmutable;
  toda normalización/corrección va a `decisiones.csv` (nada en silencio). Detalle en `README.md`.
- Los pasos de **CHEQUEO (Paso 2)** y **GEO (Paso 3)** aún **NO están bien definidos** → se
  planifican en detalle al llegar a cada uno.

## Roadmap y estado
- **Paso 1 — Transcripción VERBATIM por visión.** Zona 3 (v3_p01–p23, BA+La Pampa) = 909 filas.
  **Zona 4 (Córdoba+Santa Fe) = 775. Zona 5 (Corrientes+ER+Misiones) = 304. Zona 6 (Chaco+Formosa+Sgo
  del Estero) = 285. Zona 7 (v7_p01–p11, NOA: Jujuy+Salta+Tucumán+Catamarca+La Rioja) = 430** (Jujuy 68,
  Salta 118, Tucumán 102, Catamarca 77, La Rioja 65). **Zona 8 (v8_p01–p05, Cuyo: Mendoza+San Juan+San
  Luis) = 200** (Mendoza 71, San Juan 61, San Luis 68). **Zona 9 (v9_1_2_p01–p05, Patagonia: Río Negro+
  Chubut+Neuquén+Santa Cruz+Tierra del Fuego) = 160** (Río Negro 62, Chubut 45, Neuquén 30, Santa Cruz
  21, Tierra del Fuego 2). **`poblados_1960.csv` = 3.063 filas — PASO 1 COMPLETO (todo el país).**
  Pendiente: correr 1B + Paso 2 sobre zonas 4–9 (pase conjunto).
  - **FRENAR zona 9:** 0 vacíos, 0 sumas erróneas. Deptos cuadran con vintage 1960; near-variants verbatim
    para 1B: Chubut `Río Senguer`~`Río Senguerr`; Río Negro `Pichi-Mahuida`~`Pichi-Mahuída`; Santa Cruz
    `Corpen Aike`~`Corpen-Aike`. (Pág. `v9_1_2_p05` = cola V-Y-Z, solo 6 filas.)
  - **FRENAR zona 8:** 0 vacíos, 0 sumas erróneas. Deptos cuadran con vintage 1960: Mendoza 18 ✓; San
    Luis 9 ✓; **San Juan 20→18** unificando en 1B `Iglesia`~`Iglesias` y `Veinticinco de Mayo`~`Venticinco
    de Mayo` (misprint de Zonda, ambos verbatim). OJO v8_p04: la media-página recorta el dígito de las
    centenas de `n_orden` (crop empieza en x=25) → n_orden re-leídos de la página completa `pages/v8_p04.png`.
  - **FRENAR zona 7 (verificación):** 0 campos vacíos; deptos distintos por prov cuadran con vintage 1960
    tras 1B (Jujuy 15; Tucumán 11; Catamarca 16; **La Rioja 19→18** unificando `General Gordillo`(p02
    Bella Vista) ~ `Gobernador Gordillo`(p03 Chamical) —AMBOS verbatim, flag near-variant 1B—; **Salta
    24→23** unificando `Caldera`(p07/p11) ~ `La Caldera`(p05) —AMBOS verbatim, flag 1B—). 2 sumas
    total≠v+m = mis-lecturas mías de un dígito, re-leídas contra la imagen y anotadas en
    `dudas_transcripcion.csv` para corregir en Paso 2.1: Chumbicha (v7_p03/65) varones 1.092→**1.091**;
    Perico (v7_p08/38) varones 2.018→**2.038**. Otras dudas p07: Miraflores total 587 (1er dígito
    borroso, suma confirma); `Palta de Aparzo`(l/t; topónimo real Palca) → Paso 3.
- **Paso 1B — Vocabulario controlado JERÁRQUICO — HECHO (país completo, zonas 3–9).** Corrido
  `build_autoridad.py` sobre el crudo entero (497 pares, 23 provincias). 19 flags: zona 3 ya resuelta;
  varios falsos positivos (Pila~Pilar, Colón~Morón, Puán~Luján, Moreno~Loreto). **José confirmó 7
  unificaciones nuevas** (`decisiones.csv` paso=1B): Chubut `Río Senguerr`→**Río Senguer**; Río Negro
  `Pichi-Mahuída`→**Pichi-Mahuida**; Salta `Caldera`→**La Caldera**; San Juan `Venticinco`→**Veinticinco
  de Mayo** y `Iglesias`→**Iglesia**; Santa Cruz `Corpen-Aike`→**Corpen Aike**; La Rioja `General
  Gordillo`→**Gobernador Gordillo** (este no lo detecta Levenshtein>2; se agregó a mano). `autoridad_depto.csv`
  finalizado (overrides + re-aplicadas las de zona 3); deptos canónicos cuadran con vintage 1960 (Chubut
  16, Río Negro 13, Salta 23, San Juan 18, Santa Cruz 7, La Rioja 18). Volcado a `vista_ancha.csv`.
- **Paso 1B (histórico zona 3).** Algoritmo en README ("PASO 1B").
  `autoridad_prov.csv` (2) + `autoridad_depto.csv` (146 pares). Unificó Cura-Có, Loventué, Madariaga
  (3→1), expandió Cnel. de Mar. L.N.Rosales→Coronel de Marina Leonardo Rosales, footnote-2→nombre propio.
  Anomalía Caseros/La Pampa (=La Larga, BA) diferida a Paso 3 (no se reasigna provincia en 1B). Canónicos:
  BA 120, La Pampa 23 (→22 al resolver Caseros). Volcado a `vista_ancha.csv`.
- **Paso 2 — Checks — CERRADO PAÍS COMPLETO (2026-07-15).** Algoritmo determinístico en `README.md`
  ("PASO 2 — ALGORITMO DE CHECKS"). Núcleo: capa automática 100% determinística que solo PARTE filas en
  `pasa-limpio`/`flag` (NO auto-corrige); **todo `flag` → revisión humana (2.3)**, único punto de decisión;
  crudo inmutable. Sub-pasos: 2.0 dudas · 2.1 sumas · 2.2 nombres-Wikipedia · 2.4 depto-xlsx · 2.3 humano.
  - **CIERRE (todas las zonas):** `cola_humana.csv` = **0 open / 742 resuelto**. La cola se rediseñó a
    **`flags − ledgers`** con columnas `estado`/`resuelto_por` (la Capa 1 sigue stateless; el estado
    "resuelto" vive en ledgers append-only). Ledgers de la Capa 2: `decisiones.csv` (=64),
    `revision_aproximados.csv` (veredicto por-ítem de los `aproximado`, país completo), `dudas_resueltas.csv`
    (=13). **Regla de defer materializada:** los `sin_match`/`sin_sugerencia` (sin artículo Wikipedia = sin
    señal de typo) → `nombres_diferidos_paso3.csv` (**529**, todas las zonas; cierra el hueco solo-prosa de
    zona 3). Revisión humana completa: zonas 4–9 (134 aproximados + 6 sumas) y zona 3 (32 aproximados + 9
    dudas). Typos reales de nombre hallados en toda la revisión: 9 (6 en z4–9, 3 en z3). **Sólo 2.4 queda
    pendiente** (espera los xlsx `1c1960_3_*`); es una validación independiente que NO bloquea el Paso 3.
  - **2.1 sumas PAÍS COMPLETO (HECHO):** `check_numeros.py` sobre 3.063 filas → 17 flags (+1 incompleto).
    7 de zona 3 ya resueltos. 4 correcciones nuevas grabadas en `decisiones.csv` (mis-lecturas mías,
    imagen=autoridad): Chumbicha var 1.092→1.091, Perico var 2.018→2.038, Lucio V. López muj 455→465,
    Sunchales total 7.890→7.880. **6 dudosas → `cola_sumas_pendientes.csv`** (José revisa: la imagen
    coincide con el crudo, decidir error-de-fuente vs dígito mal leído): El Fuertecito (v4_p06, Δ+4),
    Colonia Hocker (v5_p02, Δ+32), Grutly (v4_p07, Δ+100), Saenz Valiente (v5_p06, Δ+20), Simbolar
    (v6_p06, Δ−100), Sumamao (v6_p06, Δ−80). Recortes en `pages/checks/pend_*.png`.
  - **2.2 nombres-Wikipedia PAÍS COMPLETO (parcial):** `validar_nombres_wiki.py` → 3.063 filas: **2.355
    exactos** (validado) + 708 a revisión humana (47 aproximado, 171 sin_match, 490 sin_sugerencia). OJO:
    la detección de EXACTOS es completa; las SUGERENCIAS (aproximado/sin_match) solo están completas para
    zona 3 (su `wiki_cache.json` ya existía). Para zonas 4–9 el script solo consultó opensearch de las
    "dudas" → el grueso de no-exactos cayó en `sin_sugerencia` (488). Aproximados nuevos (4–9) = 2:
    **Irivilli** (v4_p08) ~ Inriville [posible typo, revisar imagen]; **Palta de Aparzo** (v7_p07) ~ Palca
    de Aparzo [ya en `dudas_transcripcion.csv`, difiere a Paso 3]. **HECHO:** `enrich_wiki.py` corrido
    (cache 414→900) + 2.2 re-validado; los 179 aproximados (país completo) revisados uno-por-uno contra
    imagen → `revision_aproximados.csv`; los `sin_match`/`sin_sugerencia` diferidos en masa a Paso 3.
  - **Estado (histórico zona 3): CERRADO** salvo 2.4. Automáticos + revisión humana 2.3 hechos.
  2.1: 5 correcciones + 2 inconsistencia_fuente. 2.2: 692 exactos, 13 dudosos revisados (2 typos), 204
  nombres diferidos a Paso 3 (Georef). 2.0: 6 dudas resueltas (Denhny→Dennhy). 2.4 diferido (faltan xlsx
  `1c1960_3_*`). Próximo: Paso 1B o zonas 4–9.
- **Paso 3 — Georreferenciación Georef/INDEC ← PASO ACTUAL** de TODAS las localidades (correspondencia
  (prov,depto)↔INDEC, match, casos cambio/fusión/subdivisión, trazable). **ALGORITMO ESCRITO** en
  `README.md` ("PASO 3 — ALGORITMO DE GEORREFERENCIACIÓN", al nivel del Paso 2). Motor:
  `scripts/geocode_georef.py` + API pública de Georef (`apis.datos.gob.ar/georef/api`, POST batch), cache
  `georef_cache.json`. **Decisiones fijadas con José (2026-07-15):** (1) granularidad = cascada
  `localidades-censales`→`localidades`/`asentamientos` (BAHRA); (2) match perfecto en 3 campos
  (localidad+depto+prov), depto 1960→moderno vía **crosswalk INDEC nuevo e independiente** (3.1; Georef
  `/departamentos` + investigación web trazable; NO se ata a `geolev2`; cotejo con `apply_name_changes()`
  del repo); (3) auto-aceptar exactos, resto → humano (3.3); (4) footnote(1) conurbano → 1 punto =
  cabecera del partido; (5) provincias homogeneizadas a Georef (3.0; único caso Tierra del Fuego; CABA no
  aplica). **Sub-pasos:** 3.0 provincia · 3.1 crosswalk depto · 3.2 match localidad (con 2 pilotos de
  tuning: 1 depto aleatorio → 1 provincia, luego corrida limpia) · 3.3 humano/caso-a-caso · 3.4 ensamble →
  `coordenadas_1960.csv` (VARIAS por depto). **Mismatches:** la capa auto solo PARTE (`auto_ok`/`flag`);
  todo `flag` lo resuelve el humano en 3 puntos (3.0 raro / 3.1 deptos / 3.3 localidades); sin fuente no
  hay coordenada. Plan de sesión: `C:\Users\josem\.claude\plans\retomo-el-proyecto-de-lazy-hammock.md`.
  - **HECHO:** 3.0 provincia (`autoridad_prov.csv`+Georef; TdF confirmada; sin CABA). 3.1 crosswalk
    (`crosswalk_indec.csv`, 490 filas = 462 identidad + 19 rename + 5 split + 3 especial + 1 sin_equivalente;
    los 45 flags resueltos con José, fuente web para renames/splits). Motor `geocode_georef.py` extendido
    (depto+exacto+max>1+cascada+candidatos, cache firma completa). `scripts/`: build_prov_georef,
    build_crosswalk_indec, apply_crosswalk_flags, build_geo_match.
  - **PILOTOS 3.2 (semilla 1960) HECHOS:** A=Córdoba/San Alberto (12 loc → 11 auto_ok + 1 flag_bahra);
    B=San Luis (68 loc → 48 auto_ok + 7 flag_bahra + 7 flag_variante + 6 flag_sin_match). **Ajustes al
    algoritmo:** (1) la cascada BAHRA **mantiene el departamento** (sin eso devolvía homónimos de otros
    deptos); (2) nuevo tier **`flag_variante`** (fuzzy en el depto esperado) que separa variantes de nombre
    de los misses reales. Congelado en README ("PASO 3", 6 desenlaces).
  - **CORRIDA MASIVA 3.2 HECHA (1ª vuelta):** 3.063 → auto_ok 1915 (62.5%) · flag_bahra 530 ·
    flag_sin_match 341 · flag_variante 200 · flag_depto 76 · flag_ambiguo 1.
  - **⚠ PROBLEMA DE MÉTODO DETECTADO → ver `PROBLEMAS_paso3.md`.** 3.1 testeó persistencia de NOMBRE, no
    TERRITORIAL: un depto puede conservar el nombre y subdividirse. **De 46 padres subdivididos, 42 estaban
    marcados `identidad`.** Los splits quedaron sin modelar; `flag_depto` los destapó de casualidad,
    mezclados con homónimos (y cazó un typo: Vélez Sarsfield → `Ángel Vicente Peñaloza` [46056]).
  - **DECISIÓN (José):** se mantiene el **match exacto en 3 campos** pero **tras acomodar la historia**:
    el crosswalk define un **CONJUNTO PERMITIDO** por depto 1960 (identidad/rename→1; split→{padre+hijos})
    y se exige `depto_georef ∈ conjunto`. Se descartó el "department-last" (aflojar el depto) porque
    auto-aceptaría tocayos **en silencio** (probado: `Pavón` Exaltación de la Cruz→General Lavalle ~300 km).
    Costo del método elegido = trabajo; del descartado = datos malos silenciosos.
  - **EN CURSO — 3.1b (la pieza que faltaba):** historia de deptos **sistemática y con fuente** (por
    provincia: deptos modernos creados post-1960 + su padre 1960, vía anexos Wikipedia/leyes/INDEC) →
    filas `split` en el crosswalk. Límite declarado: las **transferencias de límite entre deptos
    preexistentes** no están documentadas → residuo caso-a-caso, ordenado por **distancia entre centroides**
    de depto (evidencia dura, no decide).
  - **3.1b HECHO (historia sistemática).** 30 creaciones post-1960 en 6 provincias (BA 18, Tucumán 6,
    Entre Ríos 3, Chaco 1, Jujuy 1, TdF 1); 17 provincias 1:1. Todo con `fuente_url` (BA verificado contra
    el texto de ley). **Fuente primaria hallada:** codebook CELADE 1960 (`ref/ar60divp.pdf`) → **validó 1B:
    18/23 provincias con conteo EXACTO** y confirmó de forma independiente el artefacto `Chubut`, la
    anomalía `Caseros`/La Pampa y la doble-grafía de Santa Fe. Scripts: `parse_1960_oficial.py`,
    `diff_1960_canon.py`, `build_crosswalk_v2.py`.
  - **`crosswalk_indec.csv` v2 HECHO:** 529 filas / 487 deptos; **32 conjuntos múltiples**; 0 sin resolver;
    0 filas no-identidad sin fuente; determinístico; P3 corregido (`Ángel Vicente Peñaloza` [46056]).
  - **RE-CORRIDA MASIVA 3.2 v2 HECHA + diff QC:** `auto_ok` 1915→**1956** · `flag_depto` 76→**35** ·
    sin_match 341→321 · bahra 530→543 · variante 200→207. Solo 3 transiciones, todas explicadas
    (`flag_depto→auto_ok` 41 = los splits); **0 `auto_ok` cambió de `georef_id`**. Piloto BA: 22→6
    flag_depto, 16 splits → auto_ok. Residuo 35 ordenado por **distancia entre centroides** (<60 km =
    transferencia; ≥150 km = homónimo). Detalle y pendientes en `PROBLEMAS_paso3.md`.
  - **P6 — FIX DE CARACTERES ESPECIALES + RE-MATCH v3 (HECHO).** Georef traía un **U+00AD (soft hyphen)
    invisible** tras la `í` (`Juní­n`, `Olavarrí­a`) ⇒ `exacto=true` daba 0 hits ⇒ 18 localidades censales
    (125.463 hab, incl. **Junín** y **Olavarría**) caían a BAHRA **en silencio**. Causa raíz arquitectural:
    delegábamos la igualdad de nombres al servidor sobre datos sucios. **Arreglo:** `norm_name()` en
    `geocode_georef.py` (genérico: quita toda la categoría Unicode `Cf`) + **PROHIBIDO usar `exacto=true`
    como criterio** (se consulta sin `exacto` y la igualdad la decide nuestro código) + comparación de
    depto también normalizada. Verificado: `scripts/test_norm_name.py` (14 tests + 12 casos reales +
    no-colisión). Auditoría (`audit_caracteres.py`): Georef solo U+00AD (28); canon y crosswalk limpios.
  - **DISTRIBUCIÓN v3 (actual):** `auto_ok` **1993 (65,1%)** · `flag_bahra` 522 · `flag_sin_match` 318 ·
    `flag_variante` 192 · `flag_depto` 37 · `flag_ambiguo` 1 → **1070 flags (34,9%)**. Diff v2→v3: +37
    auto_ok (22 soft-hyphen + 15 guión↔espacio), **0 `auto_ok` alterado**; determinismo verificado.
  - **OJO (clave de fila):** `(page, n_orden)` NO es única (los footnote(1) del conurbano están en sección
    aparte con numeración propia) → usar **`(page, n_orden, localidad)`**.
- **Paso 3.3a — Investigación documentada de flags: HECHO** → `investigacion_flags.csv` (algoritmo en
  README §3.3a). Los **1070 flags** con **propuesta + fuente**, ordenados por **población 1960**.
  Cobertura verificada (0 faltantes / 0 sobrantes), **0 propuestas sin fuente**, doble corrida idéntica.
  **Alcance decidido con José:** Tier A **aparcado** (no se revisa ahora); foco en B/C/D.
  | tier | n | pob | qué |
  |---|---|---|---|
  | A (aparcado) | 696 | 773.316 | bahra/variante: el candidato Georef ES la evidencia |
  | C-generador | 39 | **358.327** | generador de candidatos (prefijo 27, nº-palabra 5, abrev 4, paréntesis 2) |
  | C5-pendiente | 266 | 307.867 | **investigación web pendiente** |
  | C1-cabecera | 11 | 210.893 | cabecera del depto, investigada con `fuente_url` |
  | B | 37 | 41.222 | transferencia vs homónimo (`dist_km`) |
  | D | 19 | 37.422 | ambigüedad (>1 candidato) |
  | C-especial | 2 | 19.050 | `manual_coord` citada |
  **Con propuesta: 748 flags = 1.361.586 hab (78% de la población flageada).** Sin propuesta = 266 de C5
  + 56 de B/D (que por diseño llevan evidencia, no candidato).
  - **Regla de diseño (README §3.3a):** *igualdad* → se arregla en 3.2 y se re-corre; *interpretación* →
    3.3a propone con fuente y José confirma. Ej: `Pcia.` significa "Provincia" el 99% de las veces pero
    acá es "Presidencia" ⇒ expandirlo automáticamente sería DECIDIR (prohibido). Por eso C1/C2/C3 **no se
    hornean en 3.2** y **no se re-corre el match**.
  - **Generador de candidatos** (`candidatos_nombre.py`): en vez de sub-clases ad hoc, genera los strings
    alternativos plausibles (paréntesis / abreviatura / número-palabra↔dígito / prefijo) y deja que Georef
    confirme, **registrando el `rationale`**. Rescató 39 flags = **40% de la población de C**.
  - **Regla footnote(1) validada por el dato:** todo partido del conurbano es **exactamente 1 localidad
    censal con el nombre del partido** (Quilmes, Morón, La Matanza, Tres de Febrero…) → por eso 16 de 18
    cayeron en `auto_ok`. `3 de Febrero` **no era** un caso de cabecera: era dígito↔palabra → localidad
    censal `Tres de Febrero` [06840010] (Caseros ni siquiera es localidad censal). `General Sarmiento`
    (partido DISUELTO en 3) es el único caso real → **San Miguel** [06760010] (decisión de José: 1 punto).
  - **Ojo (documentado en las notas):** `Isla Martín García` depende del partido de **La Plata** pero está
    a **~35 km**; su coord es la **isla** (-34.1825,-58.25), no La Plata. `Zona Nacional Puerto La Plata`
    (17.338 hab) no es localidad: zona portuaria Ensenada/Berisso, coord citada, **ambigua**.
  - **P8 — extensiones sistemáticas HECHAS (2026-07-16).** Antes de mandar los 266 de C5 a investigación
    manual, se midió qué rescataba cada regla determinística (`diag_c5.py`). Destapó un **hueco del
    algoritmo**: 3.2 era **asimétrico** (censal: exacto+fuzzy; BAHRA: **solo exacto**) ⇒ un paraje en BAHRA
    con el nombre apenas distinto caía a `sin_match` sin que nadie lo mirara.
    - **H1 → nuevo tier `flag_variante_bahra`** en 3.2 (BAHRA-fuzzy in-set, **después** del censal-fuzzy).
      Re-corrida masiva **v4**: diff **quirúrgico** — una sola transición (`sin_match→variante_bahra`,
      **68**), 2995 filas sin cambio, **0 `auto_ok` alterado**. `flag_sin_match` 318→**250**.
    - **H2/H3/H4 → generador de 3.3a** (interpretación, sin re-correr): prefijos ferroviarios
      (Desvío/Empalme/Pueblo/Kilómetro), candidatos × capas BAHRA, variantes de↔del. Generador 39→**48**.
    - **REGRESIÓN detectada y corregida:** el tier nuevo cambió el *estado* en 3.2 y **pisó una respuesta
      investigada**: `Ullún` pasó de la cabecera con fuente **Villa Ibáñez** a la propuesta mecánica
      **"Dique Ullum"** (¡una represa!). Causa: la precedencia estaba por *estado de 3.2* en vez de por
      *calidad de evidencia*. Arreglado: **cabecera/especial (investigadas con fuente) tienen precedencia
      sobre lo mecánico**.
  - **DISTRIBUCIÓN v4 de `investigacion_flags.csv` (1070 flags):**
    | tier | n | pob | qué |
    |---|---|---|---|
    | A (aparcado) | 696 | 773.316 | bahra/variante: candidato Georef = evidencia |
    | C-generador | 48 | **373.057** | generador de candidatos (con `rationale`) |
    | C5-pendiente | **190** | 250.664 | **investigación manual real** |
    | C1-cabecera | 11 | 210.893 | cabecera investigada con `fuente_url` |
    | E-bahra-fuzzy | 67 | 42.473 | **confianza BAJA** (fuzzy laxo: confirmar **o rechazar**) |
    | B | 37 | 41.222 | transferencia vs homónimo (`dist_km`) |
    | D | 19 | 37.422 | ambigüedad |
    | C-especial | 2 | 19.050 | `manual_coord` citada |
    **Con propuesta: 824 = 1.418.789 hab (81% de la población flageada).** Verificado: cobertura exacta
    (0 faltantes/0 sobrantes), **0 propuestas sin fuente**, doble corrida idéntica.
  - **C5 — investigación web con protocolo HECHA (2026-07-21).** Protocolo en README §3.3a-C5: cascada de
    fuentes (Georef→Wikipedia→OSM→gaceteros) + **guard geográfico** determinístico (Georef `/ubicacion`
    reverse-geocode: verifica que la coord caiga en el conjunto de deptos esperado). 188 investigados por
    fan-out de 8 agentes (2 bloqueados por artefacto aparte: Puerto Madryn, La Larga). Scripts:
    `guard_ubicacion.py`, `build_c5.py`; salida `c5_investigacion.csv`.
    - **Resultado:** 173 con coordenada (210.852 hab) + 15 `sin_coordenada`. Guard: **verde 161** ·
      **rojo 10** (cambios de límite/splits post-1960, no homónimos: Quequén Lobería→Necochea; La Dulce→San
      Cayetano; Miñones Federación→Federal; Aranguren Victoria→Nogoyá — a revisar) · sin_depto 2 (coord en
      borde/mar: Puerto Naranjito, Bahía Solano). **0 coords sin `fuente_url`**; doble corrida idéntica.
    - Hallazgos: el guard cazó el homónimo de `Zonda` y un error de coord de Wikipedia en `Dixonville`
      (→Fortín El Patria, San Luis). 2 bugs de comillas en CSV de agentes corregidos (Jesús María, El Recreo).
  - **DISTRIBUCIÓN v4 de `investigacion_flags.csv` (1070 flags) — CON PROPUESTA: 997 (1.629.641 hab):**
    A-aparcado 696 · C-generador 48 · C5-manual_coord 89 · C5-rename 71 · C5-barrio_de 13 · E-bahra-fuzzy 67 ·
    C1-cabecera 11 · C-especial 2. **SIN propuesta: 73** = C5-sin_coordenada 15 + B 37 + D 19 + C5-pendiente 2
    (B/D por diseño llevan evidencia, no candidato; C5-pendiente = los 2 bloqueados por artefacto).
    Verificado: cobertura 1070 exacta, 0 propuestas sin fuente.
- **Paso 3.4 — ensamble PROVISIONAL + mapa de QC (HECHO, 2026-07-22).** `coordenadas_1960.csv`: **3.063
  filas** (una por localidad), cada una con coord (si la tiene) + procedencia (estado/tier/confianza/
  fuente/fuente_url/verificado_geo). **3.044 con coordenada** (13,5M hab ubicados) · 17 sin_coordenada · 2
  bloqueado_artefacto. `verificado_geo` de las 3.044: **verde 2994** · **rojo 47** (37 flag_depto +
  10 C5: transferencias de límite/homónimos a revisar) · ambar 3. Coord `sin_depto` (mar/borde) rechazada
  por regla. **0 coords sin fuente_url**; determinístico. Scripts: `build_coordenadas.py` (resuelve coords
  de Tier A/E desde Georef cacheado) + `build_mapa_qc.py` → **`mapa_qc.html`** (SVG autocontenido, offline;
  el grueso verde recesivo, rojos/ambar resaltados; hover + lista de sin_coordenada). **ES PROVISIONAL**:
  nada confirmado por José todavía.
- **Paso 3.3 — PREPARADO para la confirmación (2026-07-22).**
  - **PASO 0 HECHO (image-confirmado, en `decisiones.csv`):** artefacto `Chubut` = **Puerto Madryn** con
    prov/depto **invertidos en la fuente** (imprimió Prov=Biedma/Depto=Chubut) → correcto Chubut/**Biedma**
    [26007020] (imagen v9_1_2_p04/16). Anomalía `Caseros` = **La Larga**: la fuente imprimió provincia
    errónea (La Pampa no tiene depto Caseros); es **BA/Daireaux** [06231040] (imagen v3_p12/482 + Georef).
    **Destraba los 2 `bloqueado_artefacto` → 0.**
  - **Auditoría (requisito de José):** `coordenadas_1960.csv` gana `criterio_deteccion`,
    `criterio_aceptacion`, `en_muestra`, `decidido_por`, `fecha`. **Distingue** `auto_match` **1993**
    (match exacto, "ya venían") de `auto_muestreo` **829** (flags a auto-aceptar por muestra) de
    `humano_lote` **172** / `humano_individual` **52** / `sin_coordenada` **17**. Cada aceptación en bloque
    será una entrada de `decisiones.csv` (no default silencioso).
  - **Entregables para José:** `revision_3.3.csv` (241 = A 52 uno-por-uno + B 172 lote + C 17 sin_coord,
    con evidencia + col `veredicto` a completar) · `muestra_paso2.csv` (30 de auto_muestreo, semilla 1960)
    · `mapa_qc.html` (3046 puntos). Scripts: `build_coordenadas.py`, `build_revision.py`, `build_mapa_qc.py`,
    `guard_ubicacion.py`.
  - **GRUPO A CERRADO vía pasada web documentada (2026-07-22, decisión de José: web en vez de revisión
    manual).** Los 37 `flag_depto` (tier B) nunca habían tenido web (parkeados con `dist_km`). Se extendió
    el protocolo web C5 al tier `flag_depto` — **§3.3b + §3.3b-2 nuevas en `README.md`**. Ejecución:
    - **§3.3b (1ª pasada, 6 agentes × 50 ítems):** cascada Georef→Wikipedia→OSM→gaceteros + **guard
      geográfico** sobre toda coord. Regla: verde→aceptar; rojo+transferencia→aceptar; sin_depto→rechazo;
      resto→residuo. Scripts `build_web33_input.py`, `build_revision_web.py`; log `revision_3.3_web.csv`
      (arrastra la flag previa; nada se pisa). Muchos "homónimos por dist_km" resultaron ser el pueblo real
      en OTRO registro del depto esperado → la distancia no decidía.
    - **§3.3b-2 (2ª pasada, doc exhaustiva de la transferencia, 3 agentes × 21):** José exigió **documentar
      el acto administrativo del cambio de depto**, no aceptar por unicidad de nombre. Resultado: **6
      `documentada`** (Quequén DL 9327/1979; Yerba Buena/Bella Vista/Río Colorado/El Manantial Ley Tucumán
      4518/1976; Gob. Racedo Ley ER 6378/1979), **1 artefacto** (Santa Teresa: fuente 1960 imprimió depto
      `Iriondo` erróneo → correcto **Constitución**; image-check confirmó), **14 `limite_no_documentada`**
      (reasignaciones entre deptos preexistentes = límite declarado §3.1b). Script `build_transfer_doc.py`
      → `transfer_doc.csv`.
    - **Chequeo de CONTIGÜIDAD (pedido de José, `check_contiguidad.py` + `ref/deptos_argentina.geojson`):**
      los 14 del bloque son **14/14 contiguos** (borde compartido, shapely) → evidencia geográfica
      independiente de la unicidad de nombre. `contiguidad_transfer.csv`.
    - **Ledger de decisiones humanas** `veredictos_3.3b.csv` (19): José ratificó el bloque de 14
      (`transferencia_limite_declarado`), aceptó Puerto de la Plata (punto del puerto, ambiguo), los 3
      chicos (Monte Cristo/Miñones/Enrique Lavalle) y Santa Teresa. **Resultado: las 50 del Grupo A
      ACEPTADAS** (31 auto-documentadas + 19 humano), 0 residuo, 0 coord sin `fuente_url`; determinístico.
    - **Volcado (`build_volcado_3.3b.py`):** `revision_3.3.csv` Grupo A = 52 `aceptar`; **`decisiones.csv`
      +50 (paso 3.3b)**. Backup `revision_3.3.prev.csv`.
    - **Re-ensamble (`build_coordenadas.py` extendido, override WEB33):** `coordenadas_1960.csv` = 3.063
      filas · **3.046 con coord (13,5M hab)** · 17 sin · **0 bloqueado** · 0 sin `fuente_url`. Los 50 pasan
      a estado **`confirmado_3.3b`** (`criterio_aceptacion` = 19 `humano_individual` + 31 `documentado_3.3b`).
      **`verificado_geo` rojo 47→23** (los homónimos → verde; los 23 rojo restantes son las transferencias
      documentadas+contiguas, TODAS explicadas). `mapa_qc.html` regenerado. Determinismo verificado.
  - **PRÓXIMO:** Grupo B (172, `humano_lote`) por vistazo al mapa + Grupo C (17 `sin_coordenada`) confirmar
    + muestra Q2 (`muestra_paso2.csv`, 30) para el bloque `auto_muestreo` (829, del que depende el flip
    `auto_muestreo`→confirmado + `en_muestra`). Tier A (696) entra como `auto_muestreo` salvo revisión.
    2.4 depto-xlsx sigue pendiente (no bloquea).

## Punto de reevaluación
- **Al cerrar ZONA 3:** evaluar calidad (checks), cobertura y costo, y recién entonces planificar
  zonas 4–9 + Paso 3. **(HECHO — Paso 1 completo, todas las zonas 3–9 transcritas = 3.063 filas.)**
- **Pase conjunto 1B + Paso 2 sobre zonas 4–9: HECHO.** Además cerrado el Paso 2 país completo (incluye
  backfill de zona 3). `cola_humana = 0 open`.
- **Paso 3 (georreferenciación): 3.0/3.1/3.1b/3.2/3.3a HECHOS; 3.4 PROVISIONAL armado; 3.3
  (confirmación humana) PREPARADO — esperando los veredictos de José.** Estado detallado arriba (bloque
  "Paso 3"). Resumen para retomar:
  - Salida provisional: **`coordenadas_1960.csv`** (3.063 filas; 3.046 con coord = 13,5M hab; 17 sin;
    columnas de auditoría `criterio_deteccion`/`criterio_aceptacion`/`en_muestra`/`decidido_por`/`fecha`;
    `auto_match` 1993 · `auto_muestreo` 829 · `humano_lote` 172 · `humano_individual` 52 · sin_coord 17).
  - **PRÓXIMO PASO EXACTO:** José completa la col `veredicto` en **`revision_3.3.csv`** (Grupo A = 52 uno
    por uno; B = 172 en `mapa_qc.html`; C = 17) y decide la muestra (**`muestra_paso2.csv`**, 30, para
    aceptar el bloque `auto_muestreo` = Q2). Luego correr **PASO 3**: re-ensamblar `coordenadas_1960.csv`
    confirmada (flip `auto_muestreo`→confirmado, marcar `en_muestra`, aplicar veredictos) y volcar **cada
    aceptación** a `decisiones.csv` (append-only; nada en silencio).
  - Pendientes menores: Tier A (696) entra como `auto_muestreo` salvo revisión; 2.4 depto-xlsx (no bloquea).
  - Problemas y lecciones del Paso 3: `PROBLEMAS_paso3.md` (P1–P8). Protocolo completo: `README.md`
    ("PASO 3 — ALGORITMO" + 3.3a-C5). Plan de sesión (copia de trabajo):
    `C:\Users\josem\.claude\plans\retomo-el-proyecto-de-lazy-hammock.md`.

## Estaciones (proceso B): DIFERIDO a otra sesión.
