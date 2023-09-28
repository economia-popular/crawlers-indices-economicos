package pib

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

type ResponseAcumuladoAno []struct {
	NtCod                 string `json:"nt_cod"`
	Nt                    string `json:"nt"`
	UgCod                 string `json:"ug_cod"`
	Ug                    string `json:"ug"`
	PCod                  string `json:"p_cod"`
	P                     string `json:"p"`
	VarCod                string `json:"var_cod"`
	Var                   string `json:"var"`
	SetoresESubsetoresCod string `json:"setores_e_subsetores_cod"`
	SetoresESubsetores    string `json:"setores_e_subsetores"`
	UmCod                 string `json:"um_cod"`
	Um                    string `json:"um"`
	V                     string `json:"v"`
}

type DataAcumuladoAno struct {
	Ano   string  `json:"ano" csv:"ano"`
	Valor float64 `json:"valor" csv:"valor"`
}

type PIBAcumuladoAno struct {
	Atualizacao   time.Time          `json:"data_atualizacao"`
	UnidadeMedida string             `json:"unidade_medida"`
	Fonte         string             `json:"fonte"`
	Data          []DataAcumuladoAno `json:"data"`
}

func RunnerPIBAcumuladoAno() {
	runnerName := "PIB - Acumulado Anual"
	url := "https://servicodados.ibge.gov.br/api/v1/conjunturais?&d=s&user=ibge&t=1846&v=585&p=190001-205701&ng=1(1)&c=11255(90707)"
	unidadeMedida := "Trilhões de Reais"
	fonte := "https://servicodados.ibge.gov.br"
	file_path := "./data/pib/pib-acumulado-ano.json"
	fileNameOutputCSV := "./data/pib/pib-acumulado-ano.csv"

	s3KeyCSV := "pib/pib-acumulado-ano.csv"
	s3KeyJSON := "pib/pib-acumulado-ano.json"

	resumido := make(map[string]float64)

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	pib := &PIBAcumuladoAno{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	pib.Atualizacao = now
	pib.Fonte = fonte
	pib.UnidadeMedida = unidadeMedida

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Realizando o request na API do IBGE para recuperar a série histórica do pib")

	res, err := http.Get(url)
	defer res.Body.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("URL", url).
			Msg("Erro ao realizar o request HTTP para o endpoint dos dados")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	var response ResponseAcumulado

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	for _, v := range response {

		valor, err := strconv.ParseFloat(strings.TrimSpace(v.V), 64)

		ano := v.PCod[0:4]

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", v.V).
				Msg("Erro ao converter o valor para Float64")
		}

		item := DataAcumuladoAno{
			Ano:   ano,
			Valor: valor,
		}

		pib.Data = append(pib.Data, item)

	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Somando os acumulos trimestrais")

	for _, v := range pib.Data {

		anoRaw := v.Ano[0:4]
		ano := anoRaw
		// ano, err := strconv.ParseInt(strings.TrimSpace(anoRaw), 10, 64)
		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", anoRaw).
				Msg("Erro ao converter o valor para Float64")
		}
		resumido[ano] += v.Valor
	}

	// Sort

	ordenado := make(map[string]float64)

	periodo := make([]string, 0, len(resumido))
	for name := range resumido {
		periodo = append(periodo, name)
	}

	sort.Strings(periodo)

	for _, name := range periodo {
		ordenado[name] = resumido[name]
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Zerando a variável temporária de DataAcumuladoAno")

	pib.Data = []DataAcumuladoAno{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Criando o resumo consolidado com o PIB Anual")

	for i, v := range ordenado {
		item := DataAcumuladoAno{
			Ano:   i,
			Valor: v,
		}

		pib.Data = append(pib.Data, item)
	}

	sort.Slice(pib.Data, func(i, j int) bool {
		return pib.Data[i].Ano < pib.Data[j].Ano
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(pib)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter a struct em JSON")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Criando arquivo de persistência para os dados convertidos")

	f, err := os.Create(file_path)
	defer f.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro ao criar o diretório para persistência dos dados")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Iniciando a escrita dos dados no arquivo de persistência")

	_, err = f.WriteString(string(b))

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	// Convertendo em CSV
	csvFile, err := os.OpenFile(fileNameOutputCSV, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao criar o dataset em CSV")
	}
	defer csvFile.Close()

	csvOutput, err := gocsv.MarshalString(&pib.Data)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao escrever o dataset em CSV")
	}

	_, err = csvFile.WriteString(string(csvOutput))
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Dataset em CSV Criado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Str("S3Key", s3KeyCSV).
		Msg("Fazendo Upload para o S3")

	err = upload.S3(fileNameOutputCSV, s3KeyCSV)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("S3Key", s3KeyCSV).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	err = upload.S3(file_path, s3KeyJSON)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("S3Key", s3KeyJSON).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Finalizado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")
}
