clear all
adopath + ../../../lib/stata/gslab_misc/ado

program main
	import excel using "..\..\..\raw_data\navigation\mainports.xlsx", sheet("consolidado_paramergeIGN") clear first
	keep if gid !=.
	keep puerto grupo gid
	destring gid, replace
	export delimited "..\temp\ports_ign.csv", replace

	import excel using "..\..\..\raw_data\navigation\mainports.xlsx", sheet("consolidado_paramergeIGN") clear first
	keep if gid==. & coordx!=. & coordy!=.
	keep puerto grupo coordx coordy

	export delimited  "..\temp\ports_forgeoref.csv", replace
end

main
