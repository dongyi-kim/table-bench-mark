# Thin convenience wrapper over scripts/run.sh
.PHONY: up gen smoke bench report shell logs ps down help

help:    ; @scripts/run.sh help
up:      ; @scripts/run.sh up
gen:     ; @scripts/run.sh gen
smoke:   ; @scripts/run.sh smoke
bench:   ; @scripts/run.sh bench
report:  ; @scripts/run.sh report
shell:   ; @scripts/run.sh shell
ps:      ; @scripts/run.sh ps
logs:    ; @scripts/run.sh logs
down:    ; @scripts/run.sh down
