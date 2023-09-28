package pib

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/gocarina/gocsv"
)

type ResponseEvolucao []struct {
	ID         string `json:"id"`
	Variavel   string `json:"variavel"`
	Unidade    string `json:"unidade"`
	Resultados []struct {
		Classificacoes []struct {
			ID        string `json:"id"`
			Nome      string `json:"nome"`
			Categoria struct {
				Num90707 string `json:"90707"`
			} `json:"categoria"`
		} `json:"classificacoes"`
		Series []struct {
			Localidade struct {
				ID    string `json:"id"`
				Nivel struct {
					ID   string `json:"id"`
					Nome string `json:"nome"`
				} `json:"nivel"`
				Nome string `json:"nome"`
			} `json:"localidade"`
			Serie struct {
				Num200001 string `json:"200001"`
				Num200002 string `json:"200002"`
				Num200003 string `json:"200003"`
				Num200004 string `json:"200004"`
				Num200101 string `json:"200101"`
				Num200102 string `json:"200102"`
				Num200103 string `json:"200103"`
				Num200104 string `json:"200104"`
				Num200201 string `json:"200201"`
				Num200202 string `json:"200202"`
				Num200203 string `json:"200203"`
				Num200204 string `json:"200204"`
				Num200301 string `json:"200301"`
				Num200302 string `json:"200302"`
				Num200303 string `json:"200303"`
				Num200304 string `json:"200304"`
				Num200401 string `json:"200401"`
				Num200402 string `json:"200402"`
				Num200403 string `json:"200403"`
				Num200404 string `json:"200404"`
				Num200501 string `json:"200501"`
				Num200502 string `json:"200502"`
				Num200503 string `json:"200503"`
				Num200504 string `json:"200504"`
				Num200601 string `json:"200601"`
				Num200602 string `json:"200602"`
				Num200603 string `json:"200603"`
				Num200604 string `json:"200604"`
				Num200701 string `json:"200701"`
				Num200702 string `json:"200702"`
				Num200703 string `json:"200703"`
				Num200704 string `json:"200704"`
				Num200801 string `json:"200801"`
				Num200802 string `json:"200802"`
				Num200803 string `json:"200803"`
				Num200804 string `json:"200804"`
				Num200901 string `json:"200901"`
				Num200902 string `json:"200902"`
				Num200903 string `json:"200903"`
				Num200904 string `json:"200904"`
				Num201001 string `json:"201001"`
				Num201002 string `json:"201002"`
				Num201003 string `json:"201003"`
				Num201004 string `json:"201004"`
				Num201101 string `json:"201101"`
				Num201102 string `json:"201102"`
				Num201103 string `json:"201103"`
				Num201104 string `json:"201104"`
				Num201201 string `json:"201201"`
				Num201202 string `json:"201202"`
				Num201203 string `json:"201203"`
				Num201204 string `json:"201204"`
				Num201301 string `json:"201301"`
				Num201302 string `json:"201302"`
				Num201303 string `json:"201303"`
				Num201304 string `json:"201304"`
				Num201401 string `json:"201401"`
				Num201402 string `json:"201402"`
				Num201403 string `json:"201403"`
				Num201404 string `json:"201404"`
				Num201501 string `json:"201501"`
				Num201502 string `json:"201502"`
				Num201503 string `json:"201503"`
				Num201504 string `json:"201504"`
				Num201601 string `json:"201601"`
				Num201602 string `json:"201602"`
				Num201603 string `json:"201603"`
				Num201604 string `json:"201604"`
				Num201701 string `json:"201701"`
				Num201702 string `json:"201702"`
				Num201703 string `json:"201703"`
				Num201704 string `json:"201704"`
				Num201801 string `json:"201801"`
				Num201802 string `json:"201802"`
				Num201803 string `json:"201803"`
				Num201804 string `json:"201804"`
				Num201901 string `json:"201901"`
				Num201902 string `json:"201902"`
				Num201903 string `json:"201903"`
				Num201904 string `json:"201904"`
				Num202001 string `json:"202001"`
				Num202002 string `json:"202002"`
				Num202003 string `json:"202003"`
				Num202004 string `json:"202004"`
				Num202101 string `json:"202101"`
				Num202102 string `json:"202102"`
				Num202103 string `json:"202103"`
				Num202104 string `json:"202104"`
				Num202201 string `json:"202201"`
				Num202202 string `json:"202202"`
				Num202203 string `json:"202203"`
				Num202204 string `json:"202204"`
				Num202301 string `json:"202301"`
				Num202302 string `json:"202302"`
				Num202303 string `json:"202303"`
				Num202304 string `json:"202304"`
				Num202401 string `json:"202401"`
				Num202402 string `json:"202402"`
				Num202403 string `json:"202403"`
				Num202404 string `json:"202404"`
			} `json:"serie"`
		} `json:"series"`
	} `json:"resultados"`
}

type Data struct {
	AnoTrimestre string  `json:"ano_trimestre" csv:"ano_trimestre"`
	Valor        float64 `json:"valor" csv:"valor"`
}

type PIB struct {
	Atualizacao   time.Time `json:"data_atualizacao"`
	UnidadeMedida string    `json:"unidade_medida"`
	Fonte         string    `json:"fonte"`
	Data          []Data    `json:"data"`
}

func RunnerEvolucaoPIB() {
	runnerName := "PIB - Variação"
	fonte := "https://servicodados.ibge.gov.br"
	url := "https://servicodados.ibge.gov.br/api/v3/agregados/5932/periodos/-400/variaveis/6561?classificacao=11255[90707]&localidades=N1"
	unidadeMedida := "Variação Trimestral"

	file_path := "./data/pib/pib-variacao.json"
	fileNameOutputCSV := "./data/pib/pib-variacao.csv"

	s3KeyCSV := "pib/pib-variacao.csv"
	s3KeyJSON := "pib/pib-variacao.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	pib := &PIB{}

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
			Msg("Erro ao realizar o request HTTO para o endpoint dos dados")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	var response ResponseEvolucao

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	// Recupera o nome dos campos da Struct onde ficam os valores do PIB
	names := structs.Names(response[0].Resultados[0].Series[0].Serie)

	for _, v := range names {
		r := reflect.ValueOf(response[0].Resultados[0].Series[0].Serie)
		f := reflect.Indirect(r).FieldByName(v)

		valueRaw := f.String()

		l.Info().
			Str("Runner", runnerName).
			Str("Valor recuperado", valueRaw).
			Msg("Convertendo valor recuperado da pib para Float64")

		if valueRaw == "" {
			l.Warn().
				Str("Runner", runnerName).
				Str("Valor recuperado", valueRaw).
				Msg("Ignorando o valor recuperado pois inda não foi contabilizado pela fonte")
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
		}

		// Formatando data
		// Convertendo para os formatos aceitos pelo Go
		// Neste momento vindo como dd/mm/yyyy, convertendo para yyyy/mm/dd por string
		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Msg("Convertendo a data para o formato 01-2006")

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Msg("Removendo patterns desnecessários da string")

		dataRaw := strings.Replace(v, "Num", "", -1)

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Str("Data sem os patterns removidos", dataRaw).
			Msg("Patterns removidos da string")

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Str("Data sem os patterns removidos", dataRaw).
			Msg("Criando os parâmetros de trimestre a partir da data")

		ano := dataRaw[0:4]
		trimestre := dataRaw[4:6]
		trimestre = strings.Replace(trimestre, "0", "", -1)

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Str("Data sem os patterns removidos", dataRaw).
			Str("Ano", ano).
			Str("Trimestre", trimestre).
			Msg("Trimestres recuperados")

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Str("Data sem os patterns removidos", dataRaw).
			Str("Ano", ano).
			Str("Trimestre", trimestre).
			Msg("Criando a label da data")

		anoTrimestre := fmt.Sprintf("%vº %s", trimestre, ano)

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Str("Data sem os patterns removidos", dataRaw).
			Str("Ano", ano).
			Str("Trimestre", trimestre).
			Str("Label", anoTrimestre).
			Msg("Label trimestre")

		item := Data{
			AnoTrimestre: anoTrimestre,
			Valor:        valor,
		}

		pib.Data = append(pib.Data, item)
	}

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
