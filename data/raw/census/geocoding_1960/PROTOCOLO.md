# Poblados 1960 — digitalización + georreferenciación (mapa del proceso)

Objetivo: digitalizar y geocodificar las **localidades** del censo argentino de 1960 a nivel
localidad — localidad · población · coordenadas — permitiendo **VARIOS puntos por departamento**
(NO un único centro). Esto representa la **geografía económica intra-departamental** y permite
anclar el Market Access de forma más fina que un centroide único de departamento.
Todo vive en esta carpeta (gitignored, no se commitea).

> **Dos documentos:** este `README.md` = el **PROTOCOLO** (cómo transcribir, reglas, esquema,
> archivos). `ROADMAP.md` = el **PLAN** (qué, en qué orden, estado/cadencia). Ambos en este directorio.

> **CADA PASO = UN ALGORITMO (regla de planificación).** Así como el "PASO 1 — ALGORITMO DE
> TRANSCRIPCIÓN" está especificado como un procedimiento determinístico, **cada paso siguiente
> (1B, 2, 3) se define —ANTES de ejecutarse— con el mismo nivel casi-algorítmico**: pasos numerados,
> reglas IF/THEN, manejo explícito de casos/dudas y lista de PROHIBIDOS. **No se ejecuta un paso sin
> ese detalle.** (Planificar el paso ⇒ producir su algoritmo y dejarlo escrito acá.)

## Protocolo (TRAZABLE) — principio rector
**Se transcribe VERBATIM lo impreso; toda normalización/corrección es un paso SEPARADO,
REGISTRADO en `decisiones.csv` y reversible (el valor crudo nunca se pierde).**

### SEPARACIÓN DE PASOS (REGLA DURA — no negociable)
**El Paso 1 es SOLO transcripción verbatim. Durante el Paso 1 NO se hace NINGÚN check:**
- ❌ NO chequear sumas (`total = varones + mujeres`) → eso es Paso 2.1.
- ❌ NO pedir confirmación humana por página → eso es Paso 2.3.
- ❌ NO consultar Wikipedia ni geocodificar → Paso 2.2 / Paso 3.
- ✅ Lo único permitido en Paso 1: leer con legibilidad, **zoom** en celdas dudosas, y **anotar
  dudas en `dudas_transcripcion.csv`** (NO se corrigen ni confirman; se resuelven en Paso 2).
Los checks corren TODOS en el Paso 2, después de terminar la transcripción de la zona.

- No modernizar, no expandir abreviaturas, no "corregir" ortografía durante la transcripción.
- Números: dígitos impresos como enteros (sin separador de miles). Si falta/ilegible → vacío.
- La forma estándar de la LOCALIDAD NO se inventa: se define al geocodificar contra Georef/INDEC
  (Paso 3), registrando `localidad_cruda → nombre_oficial + id_INDEC`.
- **Vocabulario controlado (jerárquico):** provincia = conjunto **CERRADO de 24** (provincias +
  territorios 1960; Tierra del Fuego = Territorio Nacional). Departamento/partido = lista **ÚNICA POR
  PROVINCIA** → clave `(provincia, departamento)` (por homónimos: 25 de Mayo, San Martín, etc.).
  **Esta lista canónica se construye/aplica en el Paso 1B, NO en el Paso 1** — el Paso 1 transcribe
  VERBATIM (provincia incluida); no se snap-ea a la lista durante la transcripción.

## PASO 1 — ALGORITMO DE TRANSCRIPCIÓN (determinístico — seguir al pie de la letra)
**Entrada:** las mitades `pages/halves/v{Z}_p{NN}_h1.png` y `_h2.png` de la página.
**Salida:** filas en `poblados_1960.csv` (esquema: `page,n_orden,provincia,departamento,localidad,footnote,total,varones,mujeres`).

Por cada página:
1. Abrir h1 y h2 (las mitades; son más legibles que la página completa).
2. Identificar las filas. Cada renglón = `Nº orden | Localidad | Provincia | Departamento | Total | Varones | Mujeres`.
3. **COSTURA:** si la ÚLTIMA fila de h1 y la PRIMERA de h2 tienen el **mismo Nº orden** → es la misma
   fila, **NO duplicar**. Si tienen Nº orden distinto → son contiguas (incluir ambas).
4. Recorrer las filas de **ARRIBA hacia ABAJO**. Por cada fila, leer los 7 campos **VERBATIM**:
   - `n_orden`: el número de la 1ª columna, tal cual.
   - `localidad`, `departamento`: texto **TAL CUAL impreso** (acentos, abreviaturas, ortografía
     histórica). PROHIBIDO modernizar / expandir / corregir.
   - `provincia`: **tal cual impreso** (verbatim; NO snap a la lista canónica — eso es Paso 1B).
   - `total`, `varones`, `mujeres`: dígitos impresos; quitar el `.` de miles → **ENTERO**;
     celda vacía/ilegible → **vacío**.
   - `footnote`: si hay un marcador `(k)` junto a un campo → `footnote = k` y se **QUITA** `(k)` del nombre.
5. Escribir la fila a `poblados_1960.csv`.

**REGLA DE DUDA (árbol) — si NO leés una celda con confianza:**
   a. Hacer **ZOOM** de esa celda (recorte ampliado).
   b. ¿Confianza ahora? **SÍ** → registrar lo leído. **NO** → registrar tu mejor lectura **Y** agregar
      una fila a `dudas_transcripcion.csv`.
   c. **NUNCA** inferir/inventar un valor desde conocimiento externo. **Solo se registra lo que se VE.**

**SEÑALES DE INTEGRIDAD** (solo disparan RE-MIRAR/zoom; NO corrección):
   - `n_orden` repetido o salteado dentro de la zona → zoom de esa fila.
   - ruptura del orden alfabético → zoom (bandera **débil**; el orden NO es perfecto: hubo "Balsa"
     antes de "Balneario").

**PROHIBIDO en el Paso 1** (todo esto es Paso 2 o posterior):
   - ❌ chequear `total = varones + mujeres`        - ❌ pedir confirmación humana por página
   - ❌ consultar Wikipedia / geocodificar          - ❌ normalizar/corregir/expandir nombres o snap a lista canónica
   - ❌ inferir un valor por conocimiento sin verlo en la imagen

**CADENCIA:** transcribir en lotes (varias páginas por turno), **sin** confirmación por página. Al
terminar la ZONA, **FRENAR** → reevaluar y planificar el Paso 2.

## PASO 1B — ALGORITMO: vocabulario canónico jerárquico (determinístico)
**Objetivo:** fijar la clave canónica `(provincia, departamento)` deduplicando variantes de grafía DENTRO
del censo 1960. **NO** mapea nombres 1960→modernos (renames tipo Caseros→Daireaux): eso es Paso 3 (Georef).
Mismos principios que el Paso 2: el crudo es inmutable; la capa automática solo PARTE en `pasa-limpio`/
`flag`; **todo `flag` → revisión humana** (único punto de decisión); toda decisión → `decisiones.csv`.
Forma canónica = grafía 1960 linda (acentos) + `clave_norm` (MAYÚSCULA sin acentos/espacios/puntuación).

- **1B.0 Provincia (auto):** set cerrado de 24 provincias/territorios 1960. Normalizar `provincia` cruda →
  canónica. (Zona 3: Buenos Aires, La Pampa — ya canónicas.) → `autoridad_prov.csv`.
- **1B.1 Clustering depto (auto, por provincia):** normalizar `departamento` (`clean_name`: NFKD sin
  acentos, casefold, `-`/`.`→espacio, colapsar). Grafías que comparten forma normalizada → cluster. 1
  grafía → canónico tentativo (`pasa-limpio`); >1 grafía → `flag` (humano elige). *(Cura-Co/Cura-Có.)*
- **1B.2 Near-variants (auto):** Levenshtein entre claves normalizadas dentro de la provincia; dist ≤ 2 →
  `flag` (posible mismo depto). *(Leventué/Loventué.)*
- **1B.3 Validez (auto + eyeball humano):** lista ordenada por provincia (canónico + conteo) para revisión.
  Inválidos a `flag`: `Caseros` en La Pampa (= La Larga, Daireaux/Caseros, Buenos Aires); deptos vacíos
  footnote-2 (`Isla Martín García`, `Puerto de la Plata`); abreviatura `Cnel. de Mar. L.N.Rosales`.
- **1B.4 Resolución humana:** por cada `flag`, José fija `(provincia, departamento)` canónico →
  `decisiones.csv` (paso=1B) + `autoridad_depto.csv` (`provincia, departamento_crudo,
  departamento_canonico, clave_norm, status, motivo`). Luego se vuelca `*_canonico` a `vista_ancha.csv`.

## PASO 2 — ALGORITMO DE CHECKS (determinístico — seguir al pie de la letra)
**Entrada:** `poblados_1960.csv` (crudo), `dudas_transcripcion.csv`, imágenes `pages/`, xlsx de control
`1c1960_3_*` (cuando estén disponibles). **Salida:** logs `validacion_*.csv`, `cola_humana.csv`
(con `estado`/`resuelto_por`), `revision_aproximados.csv`, `nombres_diferidos_paso3.csv`,
`flags_filas.csv`, `decisiones.csv` (append), `vista_ancha.csv`. **El crudo NO se toca nunca.**

### Origen de las dudas y qué es "no dar confianza" (honesto)
- **"No confianza" = JUICIO subjetivo del transcriptor** (modelo de visión) de que un glifo/celda no se
  lee de forma inequívoca. **NO es una fórmula ni es 100% reproducible** — es inherente a leer escaneos.
- Las dudas del Paso 1 se generan así. Gatillos típicos (taxonomía): **glifo/letra ambigua** (o/e, a/n…),
  **letra chica/comprimida**, **celda vacía/ausente**, **abreviatura impresa**; + **señales de
  integridad** (n_orden repetido/salteado, ruptura del orden alfabético) que disparan re-mirar.
- **Consecuencia de diseño:** como ese corte es juicio, **NO entra en la lógica automática**. Toda fila
  donde el juicio importa va a **revisión humana (2.3)**.

### Regla de decisión (núcleo — NO negociable)
- **(A)** La capa **automática es 100% determinística**: solo **funciones puras de comparación**
  (`total==varones+mujeres`; nombre normalizado vs título de Wikipedia cacheado; suma de `total` por depto
  vs xlsx). Reproducible con caché.
- **(B)** La capa automática **NO corrige ni decide valores**. Solo **PARTE** cada fila en
  **`pasa-limpio`** (supera el test exacto) o **`flag`** (no lo supera). No hay estado "resuelto-auto".
- **(C)** Ningún valor del crudo se cambia automáticamente. **No existe "auto-corrección por re-leer con
  confianza"** (eso sería la caja negra).
- **(D)** **TODO lo `flag` o no decidido → cola de revisión humana (2.3)**. El juicio vive SOLO ahí, a la
  vista (una persona mira la imagen). **La imagen impresa es la ÚNICA autoridad** sobre el valor.
- **(E)** Las referencias externas (Wikipedia 2.2, xlsx 2.4) **solo marcan, NUNCA fijan un valor**; se
  consultan **una sola vez** para TODAS las filas. 2.0 **no** hace búsquedas.

### 2.0 — Dudas: preparación de evidencia (NO resuelve, NO busca)
Por cada fila de `dudas_transcripcion.csv`: **recorte automático** de la celda por `(page, n_orden,
campo)` → `pages/checks/` (función determinística `lib_celda.py`). **Todas** van a la cola 2.3 con su
recorte. La celda vacía (`La Pastora`/mujeres) y el depto abreviado (`Cnel. de Mar. L.N.Rosales`) también
van a 2.3, etiquetadas por tipo para cierre rápido. **PROHIBIDO** inferir (p.ej. `total − varones`).

### 2.1 — Números (auto): `total == varones + mujeres`
Por fila con los 3 campos: `delta = total − (varones+mujeres)`. `delta==0` → `ok`. `delta≠0` → **`flag` →
cola 2.3** (con recorte de la fila); NO se adivina qué celda falla. Filas con numérico vacío → ya en 2.0.
Salida: `validacion_numeros.csv` (`page, n_orden, localidad, total, varones, mujeres, suma_vm, delta, estado`).

### 2.2 — Nombres vía Wikipedia (auto, una sola pasada) — validador PRIMARIO
Por localidad: consultar Wikipedia ES con `localidad (+ provincia, + departamento)` (**contexto depto
obligatorio** por homónimos). **Normalización fija** para comparar: NFC + casefold + quitar diacríticos +
colapsar espacios + quitar sufijos desambiguadores del título. `norm(localidad)==norm(título)` →
**`validado`** (`pasa-limpio`). Cualquier otra cosa (`aproximado` por Levenshtein, o `sin_match`) →
**`flag` → cola 2.3** con la sugerencia/URL como evidencia. Caché `wiki_cache.json`
(`localidad|provincia|departamento`). Wikipedia es SUGERENCIA, no autoridad (la oficial sale de Georef en
Paso 3). Salida: `validacion_nombres.csv` (`page, localidad, provincia, departamento, wiki_titulo,
wiki_url, tipo_match, estado`). *Nota:* parajes chicos sin artículo → `sin_match` puede ser un lote grande.

### 2.4 — Departamento (auto): total por depto vs xlsx `1c1960_3_*` (xlsx = SOLO check, nunca fuente)
Agregar `poblados_1960.csv` por `(provincia, departamento)` (las filas footnote (1) ya son el total de su
partido; sin doble conteo). Cargar xlsx (`provincia, distrito, pop`), mapear `distrito→departamento` con
el crosswalk `localidad→departamento` de la transcripción, agregar y comparar. `delta==0` → `ok`;
`delta≠0` → **`flag` → cola 2.3** (todo el depto). Salida: `validacion_depto.csv` (`provincia,
departamento, suma_transcripcion, suma_xlsx, delta, estado`).

### 2.3 — Revisión HUMANA (único punto de decisión)
**Cola consolidada** (`cola_humana.csv`) = TODO lo `flag` de 2.0/2.1/2.2/2.4, cada ítem con **recorte de
imagen** + crudo + evidencia. El usuario decide leyendo la imagen (autoridad) → `decisiones.csv`
(`paso=2.3`, `fuente=humano`). Estados: `confirmado` / `corregido` (valor_final ≠ crudo; crudo intacto) /
`inconsistencia_fuente` (la fuente no suma) / `difiere_referencia` (manda la imagen; la referencia erró).

**Modelo de cierre (dos capas, determinístico).** La capa automática (2.0/2.1/2.2/2.4) es **stateless**:
recomputa los `flag` desde el crudo inmutable y NO lee ledgers (mismo crudo → mismos flags, siempre). El
estado "resuelto" vive SOLO en **ledgers append-only** (Capa 2): `decisiones.csv`, `revision_aproximados.csv`,
`dudas_resueltas.csv`. `build_cola_humana.py` calcula la cola como **`flags − ledgers`**: a cada ítem le
agrega `estado` (`open`|`resuelto`) + `resuelto_por` (qué ledger/regla lo cerró). No descarta nada en
silencio (el "pendiente real" es `estado==open`). Sigue siendo determinístico: mismos archivos de entrada →
misma cola (verificable con doble corrida + `diff`). Mapeo origen → ledger → clave de join:

| origen | ledger que lo resuelve | clave de join |
|---|---|---|
| `2.0-duda` | `dudas_resueltas.csv` (o `decisiones.csv`) | `(page, n_orden, campo)` |
| `2.1-suma` / `2.1-incompleto` | `decisiones.csv` (paso 2.3, campo numérico) | `(page, n_orden)` |
| `2.2-aproximado` | `revision_aproximados.csv` (o `decisiones.csv` para typos) | `(page, localidad)` |
| `2.2-sin_match` / `2.2-sin_sugerencia` | **regla de defer** → `nombres_diferidos_paso3.csv` | `(page, localidad)` |
| `2.4-depto` | `decisiones.csv` (aún ninguna; 2.4 diferido) | `(provincia, departamento)` |

**Regla de defer (2.2 nombres sin artículo).** `sin_match`/`sin_sugerencia` = el nombre no tiene artículo
en Wikipedia ⇒ **sin señal de typo** (a diferencia de `aproximado`, que sí tiene un near-match que hay que
mirar). Por diseño se **difieren en masa a Paso 3** (Georef canoniza), sin revisión individual de imagen. La
regla se **materializa** determinísticamente (función pura de `validacion_nombres.csv`, todas las zonas) en
`nombres_diferidos_paso3.csv` = insumo para Paso 3, NO decisión humana. Los `aproximado`, en cambio, SÍ se
revisan uno por uno contra la imagen y su veredicto se registra en `revision_aproximados.csv`.

### Marcado footnote (1) (determinístico)
`footnote=="1"` → bandera `tipo=total_partido` en `flags_filas.csv` (18 partidos del conurbano = una fila
con el total del partido). Sin tocar el crudo. Geocoding/uso en MA → **se decide en el Paso 3**.

## PASO 3 — ALGORITMO DE GEORREFERENCIACIÓN (determinístico — seguir al pie de la letra)
**Objetivo:** asignar coordenadas (lat/lon) + nombre oficial + id INDEC a **CADA** una de las 3.063
localidades 1960, permitiendo **VARIOS puntos por departamento** (no un centro único), de forma trazable,
manejando cambios de nombre / fusiones / subdivisiones / fallos de match.
**Entrada:** `vista_ancha.csv` cols `*_canon` (nombre/prov/depto = crudo + `decisiones.csv` aplicadas),
`autoridad_prov.csv`, `flags_filas.csv` (footnote 1). **Salida:** `autoridad_prov.csv` (+col Georef),
`crosswalk_indec.csv`, `geo_match_simple.csv`, `crosswalk_cambios.csv`, `coordenadas_1960.csv`,
`decisiones.csv` (append, paso=3.x). Cache `georef_cache.json`. **El crudo NO se toca nunca.**

### Fuentes de autoridad y regla de decisión (núcleo — hereda el Paso 2)
- **(A) Tres autoridades, cada una en su dominio:** la **IMAGEN** impresa sigue siendo autoridad de lo
  *transcripto* (nombre 1960); **Georef/INDEC** es autoridad del *nombre oficial moderno + coordenada +
  id*; para *cambios históricos* (rename/fusión/subdivisión) la autoridad es la **investigación web
  trazable** (`fuente_url`).
- **(B) La capa automática es 100% determinística y solo PARTE**, no decide: cada ítem cae en
  **`auto_ok`** (match exacto en los 3 campos localidad+depto+prov) o **`flag`**. **Nunca inventa** una
  coordenada ni un nombre. No existe "auto-corrección".
- **(C) TODO lo `flag` → decisión humana (3.3)**, único punto donde se resuelve un mismatch. **Sin fuente
  no hay coordenada** (prohibido inventar lat/lon).
- **(D) Reproducibilidad:** las consultas a Georef se cachean en `georef_cache.json` con **clave = firma
  completa** (`capa|nombre|provincia|departamento|exacto|max`), de modo que el cache sea seguro entre
  versiones del algoritmo. Mismo input + mismo cache → misma salida (verificable con doble corrida).
- **(E) Normalización de nombres (fija, para comparar) — `norm_name()` en `geocode_georef.py` es la
  FUENTE ÚNICA DE VERDAD:** NFKD → quitar **toda** la categoría Unicode **`Cf`** (invisibles: soft hyphen
  U+00AD, zero-width U+200B–200D, BOM — genérico, no lista negra) → quitar marcas combinantes (acento-
  insensible) → unificar comillas/guiones → casefold → puntuación (`-`/`.`/`,`/comillas) → espacio →
  colapsar. (Reusar la *lógica* de `clean_district_name()` del repo para que los nombres caigan igual que
  el resto del panel; se cita, no se importa a ciegas.)
- **(F) La igualdad de nombres la decide NUESTRO código, NO la API.** **PROHIBIDO usar `exacto=true` de
  Georef como criterio de match.** Se consulta **sin `exacto`** (más recall, filtrando por cada depto del
  conjunto permitido) y la igualdad se evalúa client-side con `norm_name()` — tanto para la **localidad**
  como para el **departamento** (`depto ∈ conjunto`). *Motivo (P6, ver `PROBLEMAS_paso3.md`): los datos de
  Georef traen un U+00AD invisible incrustado tras la `í` (`Juní­n`, `Olavarrí­a`); con `exacto=true` el
  servidor devolvía 0 hits y 18 localidades censales (125.463 hab, incl. Junín y Olavarría) caían a BAHRA
  **en silencio**. Delegar una decisión del método a la comparación de un tercero sobre datos sucios
  convierte el bug de ellos en un error nuestro.* Verificado por `scripts/test_norm_name.py`.

### 3.0 — Provincias (auto + 1 confirmación) → `autoridad_prov.csv` (col `provincia_georef`)
Mapear cada `provincia_canonica` (23) al **nombre/id EXACTO de Georef** vía `/provincias`. `norm(canon)==
norm(georef)` único → `ok` (auto). Si no resuelve 1:1 → `flag` (humano):
- **Tierra del Fuego** → Georef `"Tierra del Fuego, Antártida e Islas del Atlántico Sur"` (en 1960
  Territorio Nacional; provincializada 1990). Confirmación humana única.
Verificar que **no haya filas CABA** en el crudo (Georef la separa como `02`; no se transcribió). El
`provincia_georef` es el string que 3.1/3.2 pasan al parámetro `provincia`.

### 3.1 — Crosswalk de departamentos 1960 → INDEC moderno → `crosswalk_indec.csv`
Fijar, por cada `(provincia_canon, departamento_canon)` 1960, el/los departamento(s) INDEC moderno(s) con
su `id` (5 díg) y el **tipo de relación**, trazable.
1. Traer el set moderno: Georef `/departamentos?provincia=X&campos=id,nombre` por cada provincia.
2. **Auto-match:** `norm(departamento_canon)==norm(depto_moderno)` **único** dentro de la provincia →
   `identidad` (`auto_ok`). Cualquier otra cosa → `flag`.
3. **Resolución humana de los `flag`** (José + investigación web): clasificar y documentar con fuente:
   - `rename` — mismo depto, nombre distinto (1:1).
   - `split` — 1 depto 1960 → N modernos (produce **N filas**, una por destino).
   - `merge` — N deptos 1960 → 1 moderno.
   - `sin_equivalente` — desaparecido/anexado (sin depto moderno).
   Cotejar con la tabla curada del repo `clean_census_1960.R::apply_name_changes()` (referencia; se cita).
   **PROHIBIDO** asignar un depto moderno sin `fuente_url` para todo lo que no sea `identidad`.
   Cols: `provincia_canon, departamento_canon, tipo, depto_moderno, id_indec, fuente, fuente_url, nota`.

### 3.1b — HISTORIA de departamentos (sistemática y trazable) → filas `split` del crosswalk
**Por qué existe (lección aprendida).** 3.1 (arriba) testea **persistencia de NOMBRE**, no **persistencia
TERRITORIAL**: marca `identidad` a todo depto 1960 cuyo nombre sobrevive. Pero un depto puede **conservar
el nombre y subdividirse** (ceder territorio a un hijo nuevo) ⇒ esos splits quedan **invisibles** para el
name-match. *Medido en la 1ª corrida: de 46 padres realmente subdivididos, **42 estaban marcados
`identidad`**.* Por eso la historia se modela **por adelantado y sistemáticamente**, no de casualidad.

**Procedimiento (por provincia, determinístico en su alcance, humano en la decisión):**
1. Enumerar los deptos/partidos modernos de la provincia (Georef `/departamentos`).
2. Para cada uno, determinar **fecha de creación** y, si es **posterior a 1960**, el **depto padre** del
   que se desprendió. Fuente: anexos "Departamentos/Partidos de la provincia de X" (Wikipedia ES; traen
   fecha + ley), leyes provinciales, INDEC. **PROHIBIDO** afirmar un padre sin `fuente_url`.
3. Cada hijo post-1960 → fila `split` en `crosswalk_indec.csv`: `(prov, padre_1960) → hijo_moderno`, con
   `id_indec`, ley/año en `nota` y `fuente_url`. El **padre sobreviviente** queda también como destino.
4. Resultado: para cada `(prov, depto)` 1960 queda definido su **CONJUNTO PERMITIDO** de deptos modernos.

**Límite honesto (documentar, no ocultar).** Se documentan sistemáticamente **renames** y
**subdivisiones**. Las **transferencias de límite entre deptos preexistentes** (no crean unidad nueva;
p.ej. `Pasteur` General Pinto→Lincoln, `Arroyo Dulce` Pergamino→Salto, `Villars` Marcos Paz→General Las
Heras) **no figuran en los anexos** → NO se modelan acá; caen al residuo `flag_depto` y se resuelven
caso-a-caso en 3.3/3.3a. "Documentado ≠ infalible": con `fuente_url` por fila, todo error es trazable y
corregible (así se cazó el typo Vélez Sarsfield → `Ángel Vicente Peñaloza` [46056]).

**Discriminador del residuo (evidencia dura, NO decide).** Para cada `flag_depto` que quede, calcular la
**distancia entre el centroide del depto esperado y el del depto candidato** (Georef `/departamentos`
expone `centroide`): cerca/adyacente → **transferencia plausible**; lejos → **homónimo probable**.
Determinístico y reproducible; ordena la cola para 3.3, donde decide José.

### 3.2 — Match de localidad contra Georef → `geo_match_simple.csv`
**Regla de CONJUNTO (tras acomodar la historia):** para cada `(prov, depto)` 1960, el crosswalk (3.1+3.1b)
define el **conjunto permitido** de deptos modernos — `identidad`/`rename` → 1 elemento; `split` →
{padre-sobreviviente + hijos}. El match exige **`nombre exacto` + `provincia` + `depto_georef ∈ conjunto
permitido`**. Así los splits **dejan de generar flags falsos**, y los tocayos en deptos lejanos **siguen
flageando** (que es el punto: el depto es la única verificación independiente de que el match es el lugar
correcto — sin él, una localidad ausente de Georef con un tocayo en la provincia se aceptaría mal **en
silencio**).

Por cada localidad (fila de `vista_ancha`, usando `*_canon`):
1. Resolver el **conjunto permitido** de depto(s) moderno(s) vía `crosswalk_indec.csv`.
2. Consultar Georef **`localidades-censales`** con `nombre=localidad_canon, provincia=provincia_georef,
   exacto, max=10, campos=id,nombre,centroide.lat,centroide.lon,departamento.nombre,provincia.nombre`, y
   quedarse con los hits cuyo `departamento ∈ conjunto permitido` (**`in_set`**). *(Se consulta por
   provincia y se filtra por el conjunto en vez de pasar un solo `departamento`: un depto 1960 puede tener
   varios destinos modernos.)*
3. **Regla de decisión (IF/THEN)** — 6 desenlaces. La CASCADA y el fuzzy **siempre se filtran por el
   conjunto permitido** (crítico: sin ese filtro, BAHRA y el fuzzy devuelven homónimos de otros deptos →
   falso match silencioso):
   - **1 hit `in_set`** → **`auto_ok`** (coord + `georef_nombre` + `id_indec`). Sin humano.
   - **>1 hit `in_set`** (homónimo dentro del conjunto) → `flag_ambiguo` (con los candidatos).
   - **0 `in_set`, pero el nombre existe exacto en la prov fuera del conjunto** → `flag_depto` (candidato
     fuera del conjunto ⇒ **transferencia de límite** o **homónimo**; adjuntar la **distancia entre
     centroides** esperado↔candidato como evidencia → 3.3 decide). También **retroalimenta 3.1b** si
     revela un split no documentado.
   - **0 exacto en censal → CASCADA `in_set`:** repetir `/localidades` y `/asentamientos` (BAHRA)
     filtrando por el conjunto. Hit exacto → `flag_bahra` (coord candidata; humano confirma; anota la capa).
   - **0 exacto en toda la cascada, pero hay fuzzy `in_set` en CENSAL** (sin `exacto`) → `flag_variante`
     (variante de grafía/forma en un depto del conjunto; humano confirma el nombre — rápido).
   - **0 exacto, pero hay fuzzy `in_set` en BAHRA** → `flag_variante_bahra` (**tier agregado**; ver P8:
     antes el algoritmo era ASIMÉTRICO — censal tenía exacto+fuzzy pero BAHRA solo exacto, y un paraje con
     el nombre apenas distinto caía a `sin_match` sin que nadie lo mirara: 68 casos). Va **después** del
     censal-fuzzy (un candidato censal pesa más). **El fuzzy es LAXO** ⇒ confianza BAJA, es una propuesta
     a confirmar **o rechazar** en 3.3, no un match.
   - nada de lo anterior → `flag_sin_match` (adjuntar sugerencia fuzzy **fuera** del conjunto como
     evidencia/pista; si no hay → caso-a-caso web / `sin_coordenada`).
   **Footnote(1)** (18 filas `total_partido`): el target es la **cabecera** del partido → 1 punto
   (`estado=total_partido_cabecera`).
   Cols: `page, n_orden, localidad_canon, provincia_georef, departamento_canon, depto_moderno_esperado,
   estado, georef_capa, georef_id, georef_nombre, georef_depto, lat, lon, candidatos`.

**Tuning previo (obligatorio, NO es el run masivo):** (A) **1 departamento aleatorio** (semilla fija) →
observar los 5 desenlaces, cazar bugs; ajustar. (B) **1 provincia entera** → estresar crosswalk/homónimos/
cascada; ajustar. Los pilotos **no graban veredictos humanos**. Luego **borrar los outputs derivados de
prueba** (conservar `georef_cache.json` solo si la clave es de firma completa; si no, borrarlo) y correr
**limpio** sobre las 3.063 con el algoritmo congelado.

### 3.3a — Investigación DOCUMENTADA de flags → `investigacion_flags.csv`
Se inserta entre 3.2 (la máquina PARTE) y 3.3 (José confirma), replicando el patrón de 3.1 (flags →
investigación con fuente → confirmación humana). Por cada `flag_*` produce una **propuesta con
`fuente_url` + confianza**. **3.3a solo PROPONE; no auto-confirma nada** (todo pasa por 3.3). Sin fuente
no hay coordenada.

**LÍMITE DE ALCANCE (regla de diseño, no negociable).** 3.3a resuelve problemas de **INTERPRETACIÓN**, no
de **igualdad**:
- **Igualdad/comparación** (¿este nombre *es* el mismo string?) → se arregla en el **motor/3.2** y se
  re-corre. Ej: el soft hyphen (P6).
- **Interpretación** (¿qué *significa* lo impreso?) → 3.3a: propuesta + fuente + confirmación humana.
  Ej: `Pcia.` significa "Provincia" el 99% de las veces, pero acá es **"Presidencia"** (Presidencia Roque
  Sáenz Peña). Solo el contexto lo decide ⇒ **expandirlo automáticamente sería DECIDIR un valor**, que la
  capa automática tiene PROHIBIDO. Mismo caso: cuál parte de `Gral. Alvarado (Miramar)` es la localidad.
Por eso C1/C2/C3 **NO se hornean en 3.2** (no se re-corre el match): son decisiones por localidad que
entran en el ensamble (3.4). *Precedente: en 3.1 las abreviaturas de depto (`Pte.`→Presidente) se
resolvieron como flags con fuente, no como regla automática.*

**Salida** (ordenada por **población 1960 desc** — concentra la atención donde pesa el MA):
`page, n_orden, localidad, provincia, depto_1960, poblacion, estado_flag, tier, propuesta,
tipo_resolucion, georef_id, id_no_censal, nombre_oficial, lat, lon, fuente, fuente_url, confianza, nota`.

**Tiers (por qué evidencia los resuelve):**
- **A — Georef ES la fuente** (auto-documentado, sin web): `flag_bahra` + `flag_variante`. El registro
  Georef (nombre oficial + id + depto + coord + capa) es la evidencia. `flag_bahra` **no es un error**:
  post-fix significa "el nombre no es una localidad censal" (ver P6/P7). Marcar **`id_no_censal=true`** en
  los hits de la capa `localidades` (sus ids INDEC están corridos — P7).
- **B — evidencia dura ya calculada**: `flag_depto` → `dist_km` entre centroides (<60 km = transferencia de
  límite plausible; ≥150 km = homónimo probable). No decide: ordena la cola.
- **C — `flag_sin_match`, por sub-clase** (se clasifica ANTES de investigar; la clase es determinística):
  - **C1 — regla CABECERA**: `footnote(1)` (C1a) o `norm(localidad)==norm(departamento)` (C1b) ⇒ el censo
    listó la **cabecera** del depto bajo el nombre del depto ⇒ el target es la cabecera, **con fuente**
    (`3 de Febrero`→Caseros; `General Viamonte`→Los Toldos; `Adolfo Alsina`→Carhué).
  - **C2 — paréntesis**: el paréntesis trae la respuesta (`Gral. Alvarado (Miramar)`→Miramar). Se propone
    re-query con el contenido del paréntesis.
  - **C3 — abreviatura**: se propone la expansión (`Pcia.`→Presidencia, `Cte.`→Comandante, `Sta.`→Santa)
    + re-query. **Propuesta, no regla** (ver LÍMITE DE ALCANCE).
  - **C4 — con pista fuzzy**: revisión individual con la pista adjunta.
  - **C5 — nombre suelto**: investigación web/imagen → `rename→X` / `manual_coord` (coord citada) /
    `sin_coordenada` (documentado "no hallado"), cada uno con `fuente_url`.
- **D — ambigüedad**: `flag_ambiguo` + los `flag_bahra` con >1 candidato → desambiguar con fuente.

**PROHIBIDO en 3.3a:** auto-confirmar; expandir abreviaturas o elegir el paréntesis **sin** que quede como
propuesta a confirmar; proponer una coordenada sin `fuente`/`fuente_url`; usar `campos=completo` (P7).

#### 3.3a-C5 — PROTOCOLO de investigación de nombres sueltos → `c5_investigacion.csv`
Los C5 que ninguna regla alcanzó (nombres que no matchean Georef por ninguna transformación: estaciones
FC / parajes con nombre de persona, barrios de ciudades grandes, variantes de grafía). Fuentes **mixtas**
con **guard geográfico** (decidido con José). **La pista fuzzy adjunta es un LEAD a verificar, NO una
respuesta** (`Bermejito`→"Bermejo/Caucete", `Zonda`→"Cañada Honda" son homónimos/vecinos equivocados).

**GUARD geográfico (pieza central, determinística).** Toda coordenada propuesta —Georef, Wikipedia u OSM—
pasa por Georef **`/ubicacion?lat&lon`** (reverse-geocode) → depto del punto: ∈ **conjunto permitido** →
`verificado_geo=verde`; ∉ conjunto → `rojo` (posible homónimo → escrutinio humano, NO auto-aceptable);
sin depto (mar/exterior) → **coordenada RECHAZADA**. Hace la fuente irrelevante para la seguridad: la
coord queda verificada de forma independiente contra la geografía esperada.

**Cascada de fuentes (orden fijo, hasta el primer acierto):** 1) **Georef** amplio (token parcial, la
pista como lead) → `rename` (id censal). 2) **Wikipedia ES** (infobox con coords) → `manual_coord`.
3) **OSM/Nominatim** → `manual_coord`. 4) **gaceteros / listas de estaciones FC / sitios provinciales** →
`manual_coord`. 5) **barrio/paraje de una localidad mayor** → `barrio_de` (punto propio si existe, si no
anclar al padre, anotado). 6) **re-examen de imagen** (`lib_celda.py`) SOLO si el nombre parece mis-lectura
(Poso→Pozo) → decisión estilo Paso 2 en `decisiones.csv` + reintento. 7) nada → `sin_coordenada`
(documentado, con las fuentes consultadas).

**Ejecución:** fan-out de agentes (≈8 × ≈24 loc, salida estructurada con `fuente_url` por caso) →
**pase determinístico del guard** sobre toda coord. **Carve-out:** los **bloqueados por artefacto**
(conjunto de deptos vacío: `Puerto Madryn`/Chubut, `La Larga`/La Pampa-Caseros) NO son problema de nombre
→ `bloqueado_por_artefacto`, fuera de la cola manual; se destraban al resolver el artefacto.
Cols: `page, n_orden, localidad, provincia, depto_1960, poblacion, tipo_resolucion, propuesta, lat, lon,
fuente, fuente_url, verificado_geo, depto_del_punto, confianza, nota`.
**PROHIBIDO además:** dar por buena la pista sin verificarla; aceptar coord `verificado_geo=rojo` sin
marcarla; estimar coordenadas "a ojo" sin fuente.

#### 3.3b — PASADA WEB sobre `flag_depto` (tier B revisitado) → `revision_3.3_web.csv`
**Por qué existe (lección de método).** En 3.3a el tier **B** (`flag_depto` =
`depto_transferencia_o_homonimo`: el nombre matchea **exacto** en Georef pero en un **depto distinto** del
de 1960) se resolvió con **evidencia dura ya calculada** (`dist_km` entre centroides) y se **parkeó para
decisión humana ojo-a-ojo**, SIN pasada web. El protocolo C5 (§3.3a-C5) solo se corrió sobre los
`sin_match` (nombres sueltos). ⇒ 37 flags (41.222 hab) quedaron sin investigación documentada. **Decisión
(José, 2026-07-22):** en vez de revisión manual, **extender la investigación web documentada de C5 al
tier `flag_depto`** — es más auditable que el ojo y usa las mismas fuentes que abriría el humano. Alcance:
además de los 37 `flag_depto`, la pasada cubre los `web_*` con guard `rojo` (reconfirmar), los especiales
(`manual_coord` citada) y el `ambiguo` — toda la cola del **Grupo A** de `revision_3.3.csv` salvo los ya
resueltos en Paso 0 (`decisiones.csv`).

**Regla de auditabilidad (dura).** **Nada se pisa; todo se agrega.** El crudo y las capas previas
(`geo_match_simple.csv` 3.2, `investigacion_flags.csv`, `c5_investigacion.csv`) quedan **intactos**. La
pasada va a un **log nuevo** `revision_3.3_web.csv` que **arrastra como columnas** el contexto de la flag
previa (estado 3.2, `criterio_deteccion`, `dist_km`, candidato Georef, `verificado_geo` anterior) **+** la
capa web. Cada aceptación → **una fila en `decisiones.csv`** (append-only, `paso=3.3`, `fuente=web+url`).
Sin `fuente_url` no hay coordenada. Determinístico: doble corrida del guard/ensamble → `diff` vacío.

**Procedimiento (casi-algorítmico):**
1. **Input** (`build_web33_input.py`, determinístico, no decide): los ítems del Grupo A que necesitan web,
   con crudo `(page,n_orden,localidad,prov,depto_1960)` + candidato Georef + `dist_km` + `verificado_geo`
   previo + `poblacion` → `web33_input.csv`.
2. **Investigación** (fan-out de agentes, patrón C5): por ítem, **cascada de fuentes** Georef →
   Wikipedia ES → OSM/Nominatim → gaceteros. La pista `dist_km`/candidato es **lead a verificar, NO
   respuesta**. Clasifica `tipo_resolucion` ∈ `transferencia_confirmada` · `homonimo` · `rename` ·
   `manual_coord` · `barrio_de` · `sin_coordenada` · `residuo_humano`. Salida estructurada con
   `fuente_url` obligatorio si hay coord.
3. **Guard geográfico** (`guard_ubicacion.py`, determinístico): toda coord → Georef `/ubicacion` →
   `depto_del_punto`. ∈ conjunto permitido → `verde`; ∉ → `rojo`; sin depto (mar/exterior) → coord
   **RECHAZADA** → `sin_coordenada`. **Ojo:** una **transferencia de límite REAL cae `rojo` por
   definición** (el depto moderno no está en el conjunto documentado) ⇒ acá `rojo` NO implica error; lo
   desempata la **fuente web** (p.ej. Wikipedia "localidad del partido B, antes partido A").
4. **Regla de auto-aceptación** (determinística, en `build_revision_web.py` → `revision_3.3_web.csv`):
   - fuente **y** guard `verde` → `veredicto=aceptar`.
   - fuente **y** guard `rojo` **y** `tipo=transferencia_confirmada` con fuente explícita del cambio de
     límite → `aceptar` (marcado `frontera`; coord Georef válida).
   - fuente, pueblo 1960 no hallado/desaparecido → `veredicto=sin_coordenada` (documentado).
   - resto → `veredicto=residuo_humano` (lista corta a José; sobre todo homónimos y especiales).
5. **Residuo** → solo los `residuo_humano` van al chat con evidencia; José decide (3.3).
Cols de `revision_3.3_web.csv`: `page, n_orden, localidad, provincia, depto_1960, poblacion,
estado_flag_previo, criterio_deteccion, dist_km, candidato_georef, verificado_geo_previo,
tipo_resolucion, propuesta, georef_id, lat, lon, fuente, fuente_url, verificado_geo, depto_del_punto,
veredicto, confianza, nota`.

**PROHIBIDO en 3.3b:** dar por buena la pista (`dist_km`/candidato) sin fuente que la confirme;
auto-aceptar un guard `rojo` sin documentar la transferencia de límite con `fuente_url`; inventar/estimar
coord "a ojo"; pisar o descartar la flag previa (se arrastra en el log).

##### 3.3b-2 — DOCUMENTACIÓN EXHAUSTIVA de la transferencia (2ª pasada) → `transfer_doc.csv`
**Por qué (decisión de José, 2026-07-22).** En la 1ª pasada (§3.3b) varias `transferencia_confirmada` se
apoyaron en *"único pueblo con ese nombre en la provincia + está sobre el borde"* — evidencia de
**identidad** razonable, pero **sin citar el acto administrativo** que movió el límite. José: **eso no
alcanza para auto-aceptar**; hay que **buscar y citar la fuente del cambio**, y ese paso tiene que estar
**incorporado al algoritmo y documentado como cualquier otro**. No se acepta una transferencia por
unicidad de nombre; se acepta con **fuente del cambio** o se declara honestamente no-documentada.

**Alcance:** TODAS las filas `tipo_resolucion=transferencia_confirmada` de `revision_3.3_web.csv` (salvo
las de *cero cambio de depto*, que no son transferencias) + los residuos de tipo "posible artefacto de
depto en la fuente" (p.ej. Santa Teresa/Iriondo). Uniforme: no se privilegia la confianza de la 1ª pasada.

**Procedimiento (fan-out de agentes, búsqueda dirigida):** por ítem, buscar la **fuente
primaria/secundaria del cambio de límite** entre `depto_1960` y `depto_moderno`:
1. **Creación de depto por segregación** (el depto moderno se creó post-1960 desde el viejo): ley/decreto
   provincial con número+año, o el anexo "Departamentos/Partidos de la provincia de X" (Wikipedia ES).
2. **Reasignación/renombre** documentada (decreto de cambio de nombre o de partido de la localidad).
3. **INDEC / boletín oficial provincial / normas** (p.ej. `normas.gba.gob.ar`).
Desenlaces (`tipo_doc`):
- `documentada` — se halló la fuente del cambio (ley/decreto/creación de depto) → `fuente_cambio` +
  `fuente_url_cambio`; confianza **alta** → **aceptar**.
- `limite_no_documentada` — tras búsqueda exhaustiva NO hay un acto legal único (típico de las
  **reasignaciones de límite entre deptos preexistentes**, que el README §3.1b ya declara como **límite
  del método**: no figuran en los anexos). La **identidad** sigue sólida (nombre único en la provincia +
  coord verificada). → NO se auto-acepta: va a **residuo** como **bloque "límite declarado"**, con la
  búsqueda registrada, para ratificación humana (3.3). **REQUISITO adicional (chequeo de contigüidad,
  determinístico — `check_contiguidad.py`):** el bloque solo es aceptable si el **depto de 1960 y el depto
  moderno son CONTIGUOS** (comparten borde; polígonos de `ref/deptos_argentina.geojson` vía shapely,
  `dist_borde < 0.2 km`). Contigüidad = evidencia geográfica **independiente** de la unicidad de nombre:
  una transferencia de límite real exige adyacencia; si NO son contiguos → **homónimo**, se rechaza. Salida
  `contiguidad_transfer.csv`. (Aplicado 2026-07-22: **14/14 contiguos**, borde compartido.)
- `artefacto_fuente_depto` — el caso no era transferencia sino **depto mal impreso en la fuente 1960**
  (como Chubut/Biedma, Caseros/La Pampa): se documenta y se trata como corrección de fuente
  (`decisiones.csv`, `correccion_fuente_depto`).
Cols de `transfer_doc.csv`: `page, n_orden, localidad, depto_1960, depto_moderno, tipo_doc, fuente_cambio,
fuente_url_cambio, ley_o_anio, confianza, nota`.

**Regla de auto-aceptación actualizada (§3.3b):** una `transferencia_confirmada` se auto-acepta **solo si
`tipo_doc=documentada`**. `limite_no_documentada` → residuo (bloque declarado). Así ninguna transferencia
entra por unicidad de nombre sin que el cambio esté documentado o su límite declarado.

**PROHIBIDO:** aceptar una transferencia sin `fuente_url_cambio` salvo que quede explícitamente en el
bloque `limite_no_documentada` ratificado por José; citar una ley sin verificar que aplica a ESA localidad.

### 3.3 — Revisión HUMANA / caso-a-caso → `crosswalk_cambios.csv` + `decisiones.csv`
Cola = todos los `flag_*` de 3.2 (con la propuesta de 3.3a) + `flag` de 3.1, cada ítem con evidencia
(propuesta+fuente de 3.3a, candidatos Georef, URL, recorte
de imagen del nombre crudo si hace falta). José decide leyendo imagen + fuente web. Estados:
`confirmado` / `corregido_nombre` (nombre oficial ≠ candidato) / `rename|fusion|subdivision` (documenta el
cambio en `crosswalk_cambios.csv` con `fuente_url`) / `sin_coordenada` (desaparecida/no ubicable → queda
sin lat/lon con motivo) / `manual_coord` (coord a mano desde fuente citada). Cada decisión →
`decisiones.csv` (paso=3.3, fuente=`humano`+url). **PROHIBIDO** inventar coord sin fuente o auto-resolver
un flag. `crosswalk_cambios.csv` cols: `localidad, tipo_cambio, desde, hacia, estado, fuente_url`.

### 3.4 — Ensamble → `coordenadas_1960.csv`
Unir `auto_ok` (3.2) + propuestas (3.3a) + decisiones (3.3) → **una fila por localidad 1960** (VARIAS por
depto, no se colapsa). Las sin coordenada quedan con lat/lon vacío + `estado` + motivo (no se borran).
Cols: `page, n_orden, provincia_canon, departamento_canon, localidad_canon, footnote, total, georef_id,
nombre_oficial, georef_depto, lat, lon, estado, tier, confianza, fuente, fuente_url, verificado_geo, nota`.
`estado` ∈ `auto_ok` | `propuesto` (provisional, con su tier/confianza) | `sin_coordenada` |
`bloqueado_artefacto`. Coord por origen: auto_ok/TierA/E/B/D → `geo_match_simple.csv`; C1/C-especial/
C-generador/C5 → `investigacion_flags.csv`+`c5_investigacion.csv`. **REGLA DURA:** coord con
`verificado_geo=sin_depto` (mar/borde) se **rechaza** → sin coordenada; **0 coords sin `fuente_url`**.
**VERSIÓN PROVISIONAL** hasta la confirmación 3.3. QC visual: `scripts/build_mapa_qc.py` → `mapa_qc.html`
(SVG autocontenido, offline; puntos por `verificado_geo`, el grueso verde recesivo, rojos/ambar resaltados).
Scripts: `build_coordenadas.py`, `build_mapa_qc.py`.
Trazabilidad: cada coordenada tiene `estado` + fuente; reproducible desde crudo + cache + ledgers.

### PROHIBIDO en el Paso 3
- ❌ inventar una coordenada/nombre sin fuente (Georef o URL citada).
- ❌ auto-resolver un `flag` (todo mismatch lo decide el humano en 3.3).
- ❌ tocar el crudo `poblados_1960.csv` (correcciones → `decisiones.csv`).
- ❌ colapsar varias localidades de un depto a un único punto (el objetivo es multi-punto).
- ❌ asignar un depto moderno no-`identidad` sin `fuente_url` en `crosswalk_indec.csv`.

## Archivos
| archivo | tipo | qué es |
|---|---|---|
| `poblados_1960.csv` | **CRUDO (inmutable)** | transcripción verbatim. Cols: `page, n_orden, provincia, departamento, localidad, footnote, total, varones, mujeres` |
| `decisiones.csv` | **LOG** | toda transformación. Cols: `paso, tipo, scope, page, row, campo, valor_original, valor_final, motivo, fuente` |
| `dudas_transcripcion.csv` | **LOG (Paso 1)** | dudas de lectura anotadas al transcribir (NO resueltas). Cols: `page, n_orden, localidad, campo, nota` → las prioriza el Paso 2 |
| `notas_pie.csv` | ref | significado de cada footnote por página/zona. Cols: `page, footnote, texto, fuente` |
| `validacion_numeros.csv` | derivado (Paso 2.1) | check `total=varones+mujeres`. Cols: `page, n_orden, localidad, total, varones, mujeres, suma_vm, delta, estado` |
| `validacion_nombres.csv` | derivado (Paso 2.2) | check de nombres vs Wikipedia. Cols: `page, localidad, provincia, departamento, wiki_titulo, wiki_url, tipo_match, estado` |
| `validacion_depto.csv` | derivado (Paso 2.4) | suma por depto vs xlsx. Cols: `provincia, departamento, suma_transcripcion, suma_xlsx, delta, estado` |
| `cola_humana.csv` | derivado (Paso 2.3) | `flags − ledgers`: todo lo `flag` (2.0/2.1/2.2/2.4) con estado de cierre. Cols: `origen, page, n_orden, localidad, campo, valor_crudo, evidencia, recorte, estado, resuelto_por`. El pendiente real = `estado==open` |
| `revision_aproximados.csv` | LOG (Paso 2.2/2.3) | veredicto por-ítem de cada nombre `aproximado` revisado contra la imagen. Cols: `page, n_orden, localidad, provincia, departamento, wiki_sugerencia, dist, crop, mi_lectura, veredicto, valor_final`. `veredicto` ∈ `verbatim_paso3` / `defer_paso3_puntuacion` / `defer_paso3_wiki_ruido` / `typo_transcripcion` |
| `nombres_diferidos_paso3.csv` | derivado (Paso 2.2) | regla de defer materializada: nombres `sin_match`/`sin_sugerencia` (sin artículo Wikipedia) → canonización en Paso 3. Cols: `page, n_orden, localidad, provincia, departamento, tipo_match, motivo` |
| `dudas_resueltas.csv` | LOG (Paso 2.0) | estado de cada duda. Cols: `page, n_orden, campo, estado, nota` |
| `flags_filas.csv` | derivado (Paso 2) | banderas por fila (p.ej. `tipo=total_partido` para footnote (1)). Cols: `page, n_orden, localidad, tipo, motivo` |
| `wiki_cache.json` | caché (Paso 2.2) | respuestas Wikipedia por `localidad\|provincia\|departamento` (reproducibilidad) |
| `autoridad_prov.csv` | derivado (Paso 1B) | `provincia_cruda → provincia_canonica` |
| `autoridad_depto.csv` | derivado (Paso 1B) | `provincia, departamento_crudo → departamento_canonico` (dedupe SOLO dentro de provincia) |
| `crosswalk_indec.csv` | derivado (Paso 3.1) | `(provincia, departamento)` censo → ids INDEC/Georef |
| `geo_match_simple.csv` | derivado (Paso 3.2) | match localidad+depto contra Georef |
| `crosswalk_cambios.csv` | derivado (Paso 3.3) | cambios de nombre/fusión/subdivisión, con `estado` y `fuente_url` |
| `coordenadas_1960.csv` | **derivado final (Paso 3)** | **cada localidad** → lat/lon + nombre oficial + estado + fuente (VARIAS por departamento; no se colapsa a un único centro) |
| `vista_ancha.csv` | derivado | raw+canon lado a lado, para inspección humana |
| `pages/` | insumo | PNG de las páginas del censo; `pages/halves/` recortes ampliados; `pages/checks/` recortes de revisión |
| `scripts/` | código | pipeline reproducible |
| `README.md` | doc | **PROTOCOLO** (este mapa: cómo transcribir, reglas, esquema) |
| `ROADMAP.md` | doc | **PLAN** (qué/orden/estado/cadencia) |

## Pasos
1. **Paso 1 — Transcripción VERBATIM por visión** → `poblados_1960.csv`. **Seguir el ALGORITMO de
   arriba al pie de la letra.** SOLO transcripción; los checks son Paso 2.
1B. **Vocabulario controlado JERÁRQUICO** → ver **"PASO 1B — ALGORITMO"** arriba. Provincia = set cerrado
   de 24; departamento = lista ÚNICA por provincia (dedupe SOLO dentro de provincia) →
   `autoridad_prov.csv` / `autoridad_depto.csv` + revisión humana. Construye la clave canónica vintage-1960
   (no mapea a nombres modernos; eso es Paso 3).
2. **Checks** → ver **"PASO 2 — ALGORITMO DE CHECKS"** arriba (el procedimiento determinístico completo).
   Resumen: capa automática (2.0 evidencia de dudas · 2.1 sumas · 2.2 nombres-Wikipedia · 2.4 depto-xlsx)
   que solo PARTE filas en `pasa-limpio`/`flag`; **todo `flag` → revisión humana (2.3)**, único punto de
   decisión. El crudo es inmutable; toda decisión → `decisiones.csv`.
3. **Georreferenciación** (Georef/INDEC): 3.1 correspondencia (prov,depto)↔INDEC; 3.2 match simple;
   3.3 investigación caso a caso (cambios/fusiones/subdivisiones) trazable con fuentes web.

## Scripts (en `scripts/`)
- `render_pages.py` — rinde los PDF del censo a `pages/v*.png` (página completa).
- `make_halves.py` — regenera `pages/halves/` (recortes ampliados sup/inf) desde los PNG.
- `geocode_georef.py` — geocoder contra Georef/INDEC (Paso 3), con caché.
- `lib_celda.py` — (Paso 2) recorta la fila/celda de un `(page, n_orden, campo)` desde el half a
  `pages/checks/`. Función pura/determinística reutilizada por 2.0/2.1/2.3.
- `check_numeros.py` — (Paso 2.1) genera `validacion_numeros.csv` + recortes de los flags.
- `validar_nombres_wiki.py` — (Paso 2.2) Wikipedia ES con `wiki_cache.json` → `validacion_nombres.csv`.
- `validar_depto_xlsx.py` — (Paso 2.4) suma por depto vs xlsx `1c1960_3_*` → `validacion_depto.csv`.
- `build_cola_humana.py` — (Paso 2) consolida los `flag` de 2.0/2.1/2.2/2.4 en `cola_humana.csv`.
- `build_vista_ancha.py` — (Paso 2) ensambla `vista_ancha.csv` (crudo + decisiones humanas + flags).
- *La transcripción del Paso 1 es por VISIÓN (asistida), no un script: se leen las imágenes y se
  vuelcan las filas a `poblados_1960.csv` siguiendo el protocolo de arriba.*

## Procedencia / autocontenido
- Las imágenes `pages/v*.png` se rindieron de los tomos escaneados del Censo Nacional de Población
  1960 (zona pampeana en adelante), vía `render_pages.py`. **Se incluyen aquí** → el directorio es
  autocontenido (no depende del backup ni de otra carpeta).
- `pages/halves/` es **regenerable** con `make_halves.py` (puede borrarse para aligerar el paquete).

## Estado
- **Paso 1 — Zona 3 COMPLETA**: 909 localidades (v3_p01–v3_p23; Buenos Aires + La Pampa). 18 footnote (1)
  + 2 (2); 6 dudas.
- **Paso 1 — Zona 4 COMPLETA**: **775 localidades** (v4_p01–v4_p19; Córdoba 457 + Santa Fe 318).
  Córdoba 26 deptos (= real 1960), Santa Fe 20 (incluye variante 9 de Julio / Nueve de Julio → unifica 1B).
- **Paso 1 — Zona 5 COMPLETA**: **304 localidades** (v5; Corrientes 90 + Entre Ríos 138 + Misiones 76).
- **Paso 1 — Zona 6 COMPLETA**: **285 localidades** (v6_p01–v6_p07; Chaco 83 + Formosa 42 + Santiago del
  Estero 160). `poblados_1960.csv` acumula **2.273 filas**. Deptos: Chaco 24, Formosa 9, Sgo del Estero 27
  (vintage 1960). Falta correr 1B + Paso 2 sobre zonas 4–6; transcribir zonas 7–9.
- **Paso 2 — checks CORRIDOS + revisión humana 2.3 hecha** (`decisiones.csv` = 24 entradas; crudo intacto):
  - 2.1 sumas: 7 flags → 5 correcciones de un dígito + 2 `inconsistencia_fuente` (Irala, Villa Gessell).
  - 2.2 nombres: 692 exactos validados; 13 dudosos revisados (2 typos corregidos: Avestrus→Avestruz,
    Campo Sallee→Campo Salles; 11 verbatim-OK); **204 nombres diferidos a Paso 3** (Georef canoniza).
  - 2.0 dudas: **6 resueltas** (`dudas_resueltas.csv`): 4 confirmadas, Denhny→`Dennhy` (corregida; hoy
    Marcelino Ugarte/Dennehy → Georef Paso 3), Lia Calel mantiene crudo (incierta; Wikipedia 'Lin Calel' → Paso 3).
  - 2.4 depto: **DIFERIDO** (faltan los xlsx `1c1960_3_*` en el checkout).
  - footnote (1): 18 filas marcadas `total_partido` (`flags_filas.csv`); uso/geocoding → Paso 3.
  - Derivados: `validacion_numeros.csv`, `validacion_nombres.csv`, `cola_humana.csv`, `dudas_resueltas.csv`,
    `vista_ancha.csv`, `wiki_cache.json`.
- **Paso 1B HECHO** (zona 3): `autoridad_prov.csv` (2 provincias) + `autoridad_depto.csv` (146 pares).
  Unificaciones: Cura-Co/Có→**Cura-Có**; Leventué/Loventué→**Loventué**; 3 grafías Madariaga→**General Juan
  Madariaga**; `Cnel. de Mar. L.N.Rosales`→**Coronel de Marina Leonardo Rosales**; footnote-2 (Isla Martín
  García, Puerto de la Plata)→depto = nombre propio. **Anomalía Caseros/La Pampa** (= La Larga,
  Daireaux/Caseros BA) → marcada, **diferida a Paso 3** (no se reasigna provincia en 1B; crudo verbatim).
  Canónicos: BA 120, La Pampa 23 (→22 al resolver Caseros). Volcado a `vista_ancha.csv` (cols `*_canon`).
- **Paso 2 CERRADO** salvo 2.4 (opcional, espera los xlsx). Próximo: zonas 4–9, o correr 2.4/Paso 3 con datos.
- Cadencia: incremental (avanzar paso → evaluar → planificar el siguiente).
