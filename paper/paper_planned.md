# Transport Restructuring and Regional Development: Evidence from Argentina, 1960-1991

_Working title — alternatives under discussion._

**Authors:** José Belmar (Brown University), Diego Gentile Passaro (Brown University)

**Date:** February 25, 2026

> **This file is auto-generated** from `paper/paper_planned.pdf` via
> `paper/build_planned_md.py`. It is the read-only Markdown mirror of
> the coauthor-maintained planning PDF. Do not edit it directly; edit
> the PDF (or the source Quip) and rerun the script.
>
> The paper itself is written in LaTeX under `paper/section_*.tex`
> and compiled by `paper/paper.tex`. Those `.tex` files are the
> authoritative source for the paper that will be submitted.

---

## 1 Introduction

How do large-scale transport infrastructure changes reshape regional economies? While most research examines infrastructure expansions designed to promote development, governments often face a different challenge: fiscal consolidation that forces difficult choices about which infrastructure to maintain. Argentina's experience between 1960 and 1991 provides a unique window into this question. Facing severe fiscal pressure, the government implemented a massive transport restructuring: closing [10,000] kilometers of railway lines while expanding paved roads by [18,000] kilometers. This episode offers rare evidence on how fiscal consolidation-driven infrastructure changes--rather than development-oriented investments--affect regional market access and economic outcomes. This setting is valuable for two reasons. First, it speaks to a policy-relevant question: when governments must cut infrastructure spending, how do these cuts reshape the economic geography? The Argentine plan, motivated by fiscal rationalization rather than regional development goals, effectively acted as an unintended industrial and regional policy by shifting the dominant transport mode from rail to road. Second, it provides an opportunity to estimate how regional outcomes respond to market access changes in a multimodal infrastructure shock. Unlike typical infrastructure studies that examine single-mode expansions, Argentina's bundled rail-road restructuring allows us to study elasticities of population and sectoral employment to market access changes that integrate both transport modes. Estimating the causal effects of such infrastructure changes faces two fundamental challenges. First, infrastructure placement and removal decisions are endogenous. Even when motivated by fiscal consolidation, governments may close lines in already-declining regions or expand roads where growth is expected. This makes it difficult to separate the causal effect of market access changes from pre-existing economic trends. Second, when both rail and road networks change simultaneously, their effects are difficult to disentangle. The changes are often spatially correlated--regions losing rail access may or may not gain road access--creating multicollinearity problems if we try to estimate mode-specific effects in a single regression. Moreover, the theoretical framework

suggests that what matters for regional outcomes is total market access integrating all transport modes, not the separate contribution of each mode. This raises a methodological question: what can and cannot be identified about mode-specific effects in multimodal infrastructure episodes? This paper addresses these challenges by studying Argentina's transport restructuring through the lens of market access. We construct a unified market access index that integrates both rail and road networks using a standard spatial economics framework, computing generalized transport costs and shortest-path routes on the historical networks. Our main analysis estimates how regional outcomes--population, sectoral employment, and economic activity--respond to changes in total market access induced by the observed infrastructure plan. We then use counterfactual market access measures to explore the relative roles of rail closures versus road expansion, constructing separate scenarios where only rail changes (roads frozen) or only roads change (rail frozen). Our empirical framework follows the market access approach standard in spatial economics and trade. We model each district's economic outcomes as functions of its market access, defined as the population-weighted sum of inverse generalized transport costs to all other districts. To address endogeneity, we instrument for observed market access changes using two sources of variation. First, we exploit the Larkin Plan's technical rules: the World Bank study that motivated the reforms only examined railway segments below a specific freight threshold for potential closure, creating a discontinuity in closure probability. Second, we construct hypothetical road networks connecting major cities via least-cost paths, which predict road expansion but are orthogonal to local economic shocks. We construct a novel dataset combining historical transport networks with census data at the district level, covering [312] districts across Argentina. We validate our identification strategy with pre-period balance checks, pre-trend tests, spatial placebo tests, and over-identification tests. [Paragraph on main findings - fill in after running MA regressions. Expected: MA elasticity ≈ 0.3. Effects on sectoral composition consistent with differential transport

costs across modes.] [Paragraph on counterfactual findings - fill in after computing counterfactual MA. Expected: rail-driven MA changes account for most effects. Language: "suggestive evidence." NOTE: This is Block 2 work, to be tackled after Block 1 core is done. If we pursue sector-specific analysis, this paragraph will also need to flag that extension.] [Paragraph on mechanisms - fill in after mechanism regressions. Expected: districts losing stations/depots experienced larger declines than MA alone predicts. NOTE: Also Block 2. Same caveat.] This paper contributes to several literatures. First, we provide causal evidence on the regional effects of infrastructure disinvestment driven by fiscal consolidation. The Argentine episode involved not just the removal of rail infrastructure but a technological shift in the dominant transport mode--from rail to road--that effectively acted as an unintentional industrial policy, reshaping the country's economic geography. Second, we contribute to understanding mode-specific effects in multimodal infrastructure episodes, proposing a counterfactual market access approach that clarifies what can and cannot be identified. Third, our findings speak to contemporary policy debates about modal shifts in transport, as many countries face ongoing transitions between rail and road. Fourth, we contribute to a longstanding debate in Argentine economic history about whether railroad closures caused the decline of hinterland towns or merely followed pre-existing trends. The paper proceeds as follows. Section 2 provides historical context. Section 3 describes data and market access construction. Section 4 presents the empirical strategy. Section 5 reports main results. Section 6 explores mode-specific patterns. Section 7 examines mechanisms and heterogeneity. Section 8 concludes.

## 2 Historical Context

### 2.1 Argentina's Railway System and Fiscal Crisis

Argentina's railway network was one of the largest in the world. Construction began in 1857 and expanded rapidly through a combination of national capital, foreign investment--predominantly British and French--and direct government participation extending lines into rural areas that were not commercially profitable. By the early twentieth century, the network exceeded 40,000 kilometers, designed primarily to connect the agricultural hinterland of the Pampas, the Northwest, and the Northeast to the port of Buenos Aires. This radial, export-oriented design was both cause and consequence of the agricultural export boom of the first globalization: from 1869 to 1914, Argentine real exports grew approximately 500 percent (Fajgelbaum and Redding, 2018). By the start of the First World War, Argentina was the eighth richest country in the world. Starting in the 1930s, export growth slowed considerably and profit margins of railroad companies declined. Investment in railroad infrastructure stalled, and the aging network experienced rising operational costs. Railroads also began to face competition from the automotive industry--Ford produced the first Model T in Latin America in Buenos Aires in 1925. In 1932, Law 11.658 created a National Trunk Road System and a lump-sum fuel tax whose proceeds were allocated entirely to road construction and improvement; by 1935, there were almost 3,000 kilometers of paved roads. During the Second World War, Argentina remained neutral and its young industrial sector flourished while factories in the developed world were retooled for war production. Railroads experienced a "second life" as wartime food shortages drove up international prices for Argentine agricultural exports, helping the country accumulate reserves of $1.6 billion (Whitaker, 2013). But by 1945, industry's contribution to GDP surpassed that of agriculture for the first time (Potash, 1969). Supported by unions, the military, and the new industrial bourgeoisie, General Juan Domingo Perón reached the presidency in 1946. His economic policy rested on three pillars: strengthening the domestic market, centralized planning with state ownership

of strategic sectors, and industrial development (Whitaker, 2013; Pahowka, 2005). The showcase of this program was the acquisition and nationalization of the private railroad companies in 1947 for $600 million--a purchase widely debated at the time, with economists including Raúl Prebisch arguing that the price was excessive given the crumbling state of the infrastructure (Rock, 1985). By the early 1950s, when the fortuitous advantages of the war years had faded, Argentina's economy slipped into depression. Perón moved to protect nascent industry, armoring the automotive and petrochemical sectors against international competition. The road network continued to grow at a slow, steady pace, reaching 9,000 kilometers of paved roads by 1955. The railroad network, meanwhile, remained stable at around 43,500 kilometers, but with a shrinking budget it suffered from poor maintenance and aging equipment. In 1955, a coup removed Perón from government, and the country endured several years of political and economic instability. In 1958, Arturo Frondizi was elected president in an election from which Perón was banned. Frondizi's economic plan centered on fostering industrialization through foreign direct investment: Decree 3693 of 1959 promoted investment in the national automotive industry, and 23 new factories were approved within a year. His administration was equally aggressive in promoting road construction. In 1958, the lump-sum fuel tax was replaced by a 35 percent ad valorem tax (Decreto Nacional 505/1958), and in 1960 Law 15.274 instituted a sales tax on vehicles--in both cases, funds were permanently allocated to road construction and improvement. As for railroads, Frondizi initially sought to modernize the crumbling infrastructure, but the investment required to achieve a modern network while maintaining its current size was nearly prohibitive (López and Waddell, 2007). By 1960, annual railroad losses had reached approximately $280 million per year, accounting for almost 80 percent of the federal government deficit (Keeling, 1993).

### 2.2 The Larkin Plan (1960-1962)

In this context, the Argentine government agreed with the World Bank to commission a team of experts to evaluate the long-term economic and strategic viability of the national

transport infrastructure. The team was led by General Thomas Larkin, an American expert in military logistics who had been decorated for his efforts supplying combat troops during the Second World War and had experience consulting on railroad modernization in France, Germany, and Japan. Between 1959 and 1961, the World Bank mission scrutinized the profitability of the existing railroad and road networks. The resulting study, published by the Public Works Ministry in 1961 as Transportes Argentinos: Plan de Largo Alcance ("Argentine Transportation: Long-Run Plan"), comprised three volumes totaling approximately 3,000 pages. The first volume contained an executive summary with diagnosis, methodology, results, and recommendations organized by transport mode and time horizon; the remaining two volumes, of approximately 1,000 pages each, provided detailed analysis.1 In a world of cheap and stable oil prices, and given the poor material and financial condition of the railroad system, the plan's main recommendations were dramatic: closure of approximately 15,000 kilometers of railroad track (roughly 32 percent of the existing network), dismissal of around 70,000 railroad employees (from a total of approximately 200,000), disposal of the entire fleet of steam vehicles, closure of most national railroad workshops and factories, and aggressive improvement and construction of paved roads--in some cases to substitute for closed railroad segments, but also to create new connections in the network (Larkin, 1962). The plan proposed the construction of approximately 10,000 kilometers of paved roads over ten years. A key feature of the Larkin Plan's design is that it did not study the entire railroad network with equal intensity. Due to budget constraints and the vast size of the existing network, only railroad segments that fell below specific freight density thresholds were "studied" in detail--meaning that the experts checked each segment's explicit revenues and costs and analyzed whether closure was warranted. The thresholds were 1 million gross tons per year of cargo passing through a given segment, or 500 tons per kilometer per year of cargo entering the network through a given segment. Only 39.6 percent of the We accessed physical copies of the plan at the Mariano Moreno National Library in Buenos Aires. We scanned and digitized two key objects: a map describing the operational railroad network in 1960 (our baseline) and three tables describing the diagnosis and recommendations for each railroad segment.

railroad network was studied. For each studied segment, the experts issued one of three recommendations: maintain, close, or perform a new study. Crucially, the likelihood of the plan recommending closure was much higher for studied segments than for nonstudied ones--a discontinuity that we exploit for identification. The plan's recommendations were announced by President Frondizi in 1961, but massive protests and strikes erupted in many cities. After several months of clashes and social unrest, the government decided to abandon the plan. In the short term, only approximately 1,000 kilometers of track were closed. However, despite being abandoned after just a few months, the Larkin Plan was the only long-term comprehensive study of the national transport infrastructure for more than 30 years. It is therefore likely that its recommendations influenced national transport policy for decades ahead. As we document below, railroad closures in the 1960s and 1970s were approximately three times more likely for segments that had been studied by the plan, probably because no new large-scale study was ever undertaken to supersede it.

### 2.3 Implementation: Rail Closures and Road Expansion, 1960-

1991

The Larkin Plan marked a trend break in the evolution of both transport networks (Figure 1). For railroads, it initiated a long-term disinvestment and dismantling process. For roads, it started an almost two-decade period of aggressive construction and improvement. These large-scale changes occurred in a context of population growth (from 20 to 33 million between 1960 and 1991) and increasing GDP per capita (growing approximately 35 percent at an average annual rate of 0.95 percent). Railroad closures proceeded in two distinct waves. The first wave, from 1960 to 1966, closed approximately 4,000 kilometers of track before political opposition brought the process to a halt. The second wave came under the military dictatorship that seized power in 1976: without union resistance, the military government ordered the immediate closure of over 6,000 additional kilometers of track between 1976 and 1979. After 1979, the network stabilized--no further closures or new construction occurred between 1979

and 1986. In total, the railroad network fell from approximately 43,500 kilometers in 1960 to approximately 33,500 kilometers by 1986, a reduction of roughly 23 percent.2 Road expansion, by contrast, was continuous throughout the period. Fueled by the dedicated fuel and vehicle taxes established under Frondizi, the paved road network grew from approximately 10,000 kilometers in 1954 to approximately 28,000 kilometers by 1986--an increase of roughly 180 percent. Gravel roads also expanded, though more modestly. By 1986, the combined paved and gravel road network had reached approximately 35,000 kilometers, roughly equal to the remaining railroad network. Importantly, the spatial pattern of road expansion did not simply mirror the pattern of railroad closures: roads were not systematically built to replace closed rail lines. As we show in Section 3, the correlation between rail closures and road expansion at the district level is weak, meaning that the two shocks affected largely different sets of districts.

### 2.4 Scale Economies and Sector-Mode Specialization: Motiva-

tion and Hypotheses

A feature of the Argentine setting that motivates part of our analysis is that railroads and roads may serve different economic sectors, owing to fundamental differences in their cost structures. Railroads exhibit strong scale economies: high fixed costs (track maintenance, stations, rolling stock) but low variable costs per ton-kilometer at high traffic density. Roads, by contrast, have more constant returns to scale--per-unit costs decline less steeply with volume. The Larkin Plan itself was designed around this logic: it studied segments precisely because their traffic density was too low for rail's scale economies to be realized. Data from Baumgartner and Palazzo (1969), who estimated transport costs for Argentina using detailed operational data, are consistent with this pattern. At high cargo density (1,000 tons per day), railroad costs were 0.465 pesos per ton-kilometer compared to 1.000 for roads--rail was more than twice as cheap. At low cargo density (100 tons per We end our sample period in 1991 because there was a census in that year and because we wish to abstract from the potentially different effects of the privatizations of large fractions of the railroad and road networks during the 1990s.

day), the relationship reversed: railroad costs rose to 13.490 pesos per ton-kilometer while road costs were 8.266--road was substantially cheaper (Table 3). If these cost differences translated into actual shipping patterns, they would imply a natural sector-mode specialization: agricultural products--bulky, heavy, shipped in large volumes--would fall in the high-density regime where rail has a cost advantage, while manufactured goods--lighter, more varied, shipped in smaller batches--would fall in the low-density regime where road is cheaper. [1 paragraph on freight evidence: (1) Data from Santa Fe Railroad showing manufactured goods faced higher effective freights than agricultural goods. (2) As road network expanded, railroads specialized in bulk products--share of bulk cargo increased over time. (3) Cite slides Figures 2-5 for evidence. This paragraph depends on having access to the freight data figures. Frame as suggestive, not conclusive.] Whether these cost differences generate meaningful mode-specific effects on regional economic outcomes is ultimately an empirical question. It is not obvious a priori that they do: if road expansion sufficiently compensated for rail closures, or if firms and workers adjusted quickly to the new transport configuration, the sectoral composition of economic activity might not have changed. We treat the existence of mode-specific effects as a hypothesis to be tested in Sections 5 and 6, rather than as an established fact. The cost data presented here motivate the hypothesis but do not settle it.

Figure 1: Transport Network Changes, 1960-1986 Panel A: Railway network 1960 vs 1986. Closed segments in red. Panel B: Road network 1954 vs 1986. New segments in green. CODE: Export maps from QGIS.

Figure 1: Transport Network Changes, 1960-1986

Table 1: Summary of Network Changes

[Rows: Railroad km, Paved road km, Gravel road km. Columns: 1960, 1986, Change. Add regional breakdown.] CODE: Compute from network shapefiles.

## 3 Data

### 3.1 Transport Networks

[2-3 paragraphs describing railway data (Larkin Plan maps, 1981 National Transport Plan), road data (Automóvil Club Argentino maps 1954/1970/1986), and navigation (Rı́o de la Plata, major rivers).] WRITE: Write full data description for railways, roads, navigation.

### 3.2 Census and Economic Data

Our geographic unit is the departamento (district), using time-invariant boundaries from IPUMS. We observe [312] districts. [2 paragraphs on: (1) Population from IPUMS (1970, 1991) and digitized census (1960, 1947). (2) Employment by sector. (3) Industrial census 1954/1985. (4) Agricultural census 1960/1988. (5) Education, migration, employment status.]

Table 2: Summary Statistics

[Panel A: Network changes. Panel B: Population and employment. Panel C: Other outcomes. Columns: N, Mean, SD, Min, Max.] CODE: Compute from departments wide panel.dta.

### 3.3 Market Access Construction

3.3.1 Transport Cost Surfaces

We construct unit cost rasters that assign transport costs to each pixel based on available infrastructure. Transport costs per ton-km come from Baumgartner and Palazzo (1969) for ground transport and Larkin (1962) for navigation. Table 3 reports the cost parameters at three cargo density levels. Our main analysis uses the medium-density costs (500 tons/day), which represent a general-purpose transport cost applicable to the overall economy.

Table 3: Transport Costs by Mode and Cargo Density (1960 pesos per ton-km)

Low density Medium density High density (100 t/day) (500 t/day) (1,000 t/day) Road 8.266 1.777 1.000 Railroad 13.490 1.874 0.465 Navigation 0.621 0.621 0.621 Notes: Ground transport costs from Baumgartner and Palazzo (1969). Navigation costs from Larkin (1962). Medium-density costs used for main analysis. High- and low-density costs may be used in extensions exploring sector-specific effects (pending coauthor discussion on whether and how to introduce sector-specific MA).

At medium density, rail and road costs are similar (1.874 vs. 1.777), so the choice of mode matters less for the overall MA index. However, the cost data reveal that at high and low densities the modes diverge sharply--a pattern we return to in Section 6 when exploring whether the effects of market access changes vary across economic sectors. For pixels with both rail and road, we assign the minimum cost. Off-network travel costs [17.9] times the road cost, calibrated as the ratio of motor vehicle speed to walking speed.

3.3.2 Least-Cost Paths and Market Access

From each cost raster, we compute a transition grid converting pixel costs to conductances, allowing 8-directional movement with geographic correction for diagonal distances. We apply Dijkstra's algorithm to find the least-cost path between every pair of district centroids, yielding a 312 × 312 matrix of bilateral transport costs τijs for each sector s and time period t.

We define market access for district i at time t as:

X P opj,1960 M Ai,t = (1) j̸=i (τij,t )θ

where τij,t is the generalized transport cost between districts i and j at time t (computed using medium-density costs), and θ is the distance decay parameter. We use θ = [8.11] in our main specification and θ = [4.55] for robustness.3 Our main variable of interest is:

∆ ln M Afi ull = ln M Ai,post − ln M Ai,pre (2)

[1 paragraph acknowledging limitations: no transshipment costs at mode-switch points; single least-cost path per O-D pair. Standard in the literature but particularly relevant in our multimodal setting. NOTE: Plan is to rebuild the entire tau calculation from scratch for the new repo, possibly with a version that accounts for transshipment costs. For this draft, push with current taus and acknowledge limitations.]

Table 4: Market Access Changes

[Summary stats of ∆ ln M Af ull (overall, medium-density costs). Show N, Mean, SD, Min, Max. Sector-specific MA summary stats deferred to Section 6.] CODE: Compute from MA centroids.dta.

We hold destination populations fixed at 1960 levels to avoid endogeneity from contemporaneous population changes affecting market access. [PENDING: Justify the specific θ values. Where do they come from -- calibrated to Argentina or borrowed from literature (e.g., Donaldson and Hornbeck 2016)? Need literature review and discussion between coauthors.]

### 3.4 Descriptive Patterns

Figure 2: Infrastructure Changes and Market Access Panel A: y-axis = ∆ ln M Af ull , x-axis = ∆(km of roads in district). Panel B: y-axis = ∆ ln M Af ull , x-axis = ∆(km of railroads in district). Shows how local infrastructure changes map to the market access measure used in regressions. CODE: Create two-panel scatter from departments wide panel.dta.

Figure 2: Infrastructure Changes and Market Access

Figure 2: Spatial Distribution of Market Access Changes Choropleth map of ∆ ln M Af ull by district. CODE: Create from departments wide panel.dta + district shapefile.

Figure 3: Spatial Distribution of Market Access Changes

Figure 3: Correlation Between Rail Closures and Road Expansion Scatter: x = change in rail km, y = change in road km. Should show weak correlation. CODE: Compute from networks to districts data.

Figure 4: Correlation Between Rail Closures and Road Expansion

Figure 4: Infrastructure Changes and Market Access Panel A: y-axis = ∆ ln M Af ull , x-axis = change in road km within district. Panel B: y-axis = ∆ ln M Af ull , x-axis = change in railroad km within district. Shows how local infrastructure changes map to the MA measure we use in regressions. CODE: Create two-panel scatter from departments wide panel.dta.

Figure 5: Infrastructure Changes and Market Access

Table 5: Balance by Market Access Change Quartile

[Rows: baseline characteristics. Columns: Q1 (largest MA loss) through Q4, p-value Q1 vs Q4.] ANALYSIS: Compute quartiles of ∆ ln M Af ull , tabulate baseline characteristics.

## 4 Empirical Strategy

### 4.1 Estimating Equation

Our main specification relates changes in district-level outcomes to changes in market access: ∆Yi = β · ∆ ln M Afi ull + X′i γ + εi (3)

where ∆Yi is the change in outcome, ∆ ln M Afi ull is the change in log market access, and Xi includes baseline log market access, baseline log population, and geographic characteristics. The parameter β is the elasticity of regional outcomes with respect to market access.

### 4.2 Endogeneity Concerns

[1-2 paragraphs on: (1) Governments may close rail in declining regions (downward bias). (2) Roads built where growth expected (upward bias). (3) Direction of overall bias ambiguous.]

### 4.3 Instrumental Variables

4.3.1 Instrument 1: Larkin Plan Discontinuity

[2 paragraphs on: (1) LP only "studied" segments below freight threshold. (2) Studied segments 3× more likely to close. (3) Instrument construction. (4) Exclusion restriction.] WRITE: Formalize LP instrument construction.

4.3.2 Instrument 2: Hypothetical Road Networks

[2 paragraphs on: (1) Least-cost paths connecting major cities. (2) Euclidean and terrainbased variants. (3) Predict road expansion, orthogonal to local shocks. (4) Instrument construction.] WRITE: Formalize hypothetical road instrument.

4.3.3 First Stage

The first stage regression is:

∆ ln M Afi ull = α1 · ∆ ln M ALP i + α2 · ∆ ln M Ahypo i + X′i δ + ui (4)

### 4.4 Validation Package

Table 6: Pre-Period Balance

[Regress baseline characteristics on instruments + controls. Should show no correlation.] ANALYSIS: Run pre-balance regressions.

Table 7: Pre-Trends

[Dep var: ∆ ln population 1947-1960. Regress on instruments. Should show no effect.] ANALYSIS: Run pre-trend regressions.

Table 8: First Stage Results

[Dep var: ∆ ln M Af ull . Cols: (1) LP only, (2) Hypo only, (3) Both. Report F-stat, Hansen J.] CODE: Run first stage with MA specification.

### 4.5 Additional Robustness Approaches

We assess robustness by varying the distance decay parameter (θ), the set of controls, the sample (excluding major cities, border regions), and the time period (1960-1970,

1970-1991). Results are reported in Table 12.

## 5 Main Results

### 5.1 First Stage

[1-2 paragraphs interpreting first stage. Expected: both instruments strong, F > 10, Hansen J does not reject.]

### 5.2 Population Effects

Table 9: Effects of Market Access on Population

[Dep vars: ∆ ln total pop, urban pop, rural pop, urban share. Cols: (1) OLS, (2) IV-LP, (3) IV-Hypo, (4) IV-Both. Expected: β̂ ≈ 0.3.] CODE: Run population regressions with MA specification.

[2-3 paragraphs interpreting: OLS vs IV, magnitude, urban vs rural, comparison to Gibbons et al. (2024) and Donaldson and Hornbeck (2016).]

### 5.3 Employment and Sectoral Effects

Table 10: Effects on Sectoral Employment

[Panel A: Log levels (agriculture, manufacturing, services). Panel B: Shares. Cols: (1) OLS, (2) IV. Expected: MA loss leads to shift toward agriculture.] CODE: Run sectoral regressions with MA specification.

[2 paragraphs interpreting sectoral patterns. Do effects differ across sectors? If so, this motivates future exploration of mode-specific channels (Section 6). NOTE: Whether and how to introduce sector-specific MA remains an open question pending coauthor discussion. If sectoral effects are found here, a placeholder sentence should flag the possibility of sector-specific analysis as an extension.]

### 5.4 Other Outcomes

Table 11: Effects on Education, Migration, and Employment Status

[Dep vars: education, migration, unemployment, inactivity. Cols: OLS, IV. Expected: MA loss leads to decline in educated workers, less in-migration.] CODE: Run regressions for education, migration, employment status.

### 5.5 Robustness

Table 12: Robustness Checks

[Each column varies one aspect: (1) Baseline, (2) θ = 4.55, (3) Extra controls, (4) Excl Buenos Aires, (5) Excl border, (6) 1960-70 only, (7) 1970-91 only.] ANALYSIS: Run all robustness specifications.

## 6 Mode-Specific Patterns via Counterfactuals

NOTE: This section is a planned extension (Block 2). It will be developed after the Block 1 core (Sections 1-5) is complete and results are stable. The content below is a roadmap, not final text. The results in Section 5 establish that total market access changes have substantial effects on regional outcomes. A natural follow-up: how much is driven by rail closures

versus road expansion?

### 6.1 Counterfactual Market Access Measures

We construct three scenarios using the same MA machinery:

 ∆ ln M Afi ull : Observed change (rail closures + road expansion).

 ∆ ln M Aonly i rail : Counterfactual with only rail changes (roads frozen at 1954).

 ∆ ln M Aonly i road : Counterfactual with only road changes (rail frozen at 1960).

Table 13: Counterfactual Market Access Measures

[Summary stats: N, Mean, SD, Correlation with full.] CODE: Create 2 new cost rasters, run pipeline, compute counterfactual MA.

### 6.2 Separate Regressions

We estimate three separate specifications:

∆Yi = β f ull · ∆ ln M Afi ull + X′i γ + εi (5)

∆Yi = β rail · ∆ ln M Aonly i rail + X′i γ + εi (6)

∆Yi = β road · ∆ ln M Aonly i road + X′i γ + εi (7)

These are separate regressions, not joint. A joint specification would face severe multicollinearity.

Table 14: Population Effects Across Counterfactual Scenarios

[Dep var: ∆ ln population. Cols: (1) Full, (2) Only rail, (3) Only road. Expected: β̂ rail larger than β̂ road .] CODE: Run three separate IV regressions with counterfactual MA.

Table 15: Employment Effects Across Counterfactual Scenarios

[Same structure for sectoral employment.] CODE: Run counterfactual regressions for sectoral employment.

### 6.3 Interpretation and Caveats

[2-3 paragraphs. CRITICAL: careful language. "Suggestive evidence that rail-driven component accounts for most of the impact." Caveats: counterfactual networks never existed, decomposition not additive. What we can/cannot learn about mode-specific effects.]

Figure 4: Counterfactual Market Access Changes Panel A: ∆ ln M Af ull . Panel B: ∆ ln M Aonly rail . Panel C: ∆ ln M Aonly road . CODE: Create choropleth maps from counterfactual MA data.

Figure 6: Counterfactual Market Access Changes

## 7 Mechanisms and Heterogeneity

NOTE: This section is a planned extension (Block 2). It will be developed after the Block 1 core is complete. The content below is a roadmap, not final text.

### 7.1 Local Infrastructure Presence

We augment our main specification with local infrastructure variables:

∆Yi = β · ∆ ln M Afi ull + θ ′ Zi + X′i γ + εi (8)

where Zi includes change in road km within district, indicators for gaining/losing a national highway, gaining/losing a railway station, and losing a railway depot.

Table 16: Local Infrastructure Mechanisms - Population

[Cols: (1) Baseline, (2) +Roads, (3) +Station loss, (4) +Depot loss, (5) All Zi . Key: how much does β̂ change? Signs of θ̂?] CODE: Construct Zi variables. Run mechanism regressions.

[2 paragraphs interpreting: MA vs local services decomposition. Districts losing stations/depots have worse outcomes beyond MA.]

Figure 5: Local Infrastructure and Outcomes Scatter of ∆ ln pop vs ∆ ln M A, colored by station loss. CODE: Create scatter from regression data.

Figure 7: Local Infrastructure and Outcomes

### 7.2 Heterogeneity

Table 17: Heterogeneous Effects

[Interact ∆ ln M A with: (1) initial population, (2) agricultural share, (3) distance to port, (4) distance to Buenos Aires. Expected: larger elasticities in agricultural, less-populated, port-distant districts.] ANALYSIS: Run interaction regressions.

[1-2 paragraphs interpreting heterogeneity. MA matters more where alternatives are scarce.]

## 8 Conclusion

[Paragraph 1: What we studied. Argentina's fiscal consolidation led to massive rail closures and road expansion (1960-1991). We estimate effects using market access framework with IV.] [Paragraph 2: What we found. MA elasticity ≈ [X]. Rail-driven MA changes account for most effects. Local infrastructure amplifies MA effects. Effects persistent through 1991.]

[Paragraph 3: What it means. Fiscal consolidation-driven infrastructure cuts have lasting spatial effects. Mode substitution imperfect. Local infrastructure services matter beyond connectivity.] [Paragraph 4: Future research. Welfare analysis. Other multimodal episodes. Optimal infrastructure investment under fiscal constraints.]

References

References

Baumgartner, T. and Palazzo, J. A. (1969). Estructura Económica del Transporte de Carga Automotor y Ferroviario en la Argentina. Consejo Nacional de Desarrollo, Buenos Aires.

Fajgelbaum, P. and Redding, S. (2018). Trade, structural transformation and develop- ment: Evidence from Argentina 1869-1914. NBER Working Paper, (20217). Revised version.

Keeling, D. J. (1993). Transport and regional development in Argentina: Structural de- ficiencies and patterns of network evolution. In Yearbook, Conference of Latin Ameri- canist Geographers, pages 25-34. JSTOR.

Larkin, T. (1962). Transportes argentinos: Plan de largo alcance. Technical report, Ministerio de Obras y Servicios Públicos, Grupo de Planeamiento de los Transportes.

López, M. J. and Waddell, J. E. (2007). Nueva Historia del Ferrocarril en la Argentina:

## 150 Años de Polı́tica Ferroviaria. Lumiere, Buenos Aires.

Pahowka, G. (2005). A railroad debacle and failed economic policies: Perón's Argentina. The Gettysburg Historical Journal, 4:6.

Potash, R. A. (1969). The Army and Politics in Argentina: 1945-1962, volume 2. Stan- ford University Press.

Rock, D. (1985). Argentina, 1516-1982: From Spanish Colonization to the Falklands War. University of California Press.

Whitaker, A. P. (2013). The United States and Argentina. Harvard University Press.

A Appendix

[Table A1: Alternative distance decay parameters (θ = 4.55 and 8.11).] [Table A3: Industrial census outcomes (1954-1985).] [Table A4: Agricultural census outcomes (1960-1988).] [Table A5: Spatial autocorrelation diagnostics.] [Figure A1: Transport cost schedules (road vs rail by cargo density).] [Figure A2: Hypothetical road networks (Euclidean, LCP, MST variants).] [Figure A3: Larkin Plan studied vs non-studied segments map.]