package selic

import (
	"time"
)

type Response []struct {
	Data  string `json:"data"`
	Valor string `json:"valor"`
}

type Data struct {
	MesAno string  `json:"mes_ano"`
	Valor  float64 `json:"valor"`
}

type SELIC struct {
	Atualizacao   time.Time `json:"data_atualizacao"`
	UnidadeMedida string    `json:"unidade_medida"`
	Fonte         string    `json:"fonte"`
	Data          []Data    `json:"data"`
}

func Runner() {
	RunnerMetaSelic()
	RunnerAcumuladoMensal()
	RunnerPercentualAoAno()
}
