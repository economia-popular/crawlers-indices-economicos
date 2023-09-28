package sociais

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

type DataPobreza struct {
	Referencia string `json:"referencia" csv:"referencia"`
	Total      int64  `json:"total" csv:"total"`
}

type Pobreza struct {
	Atualizacao   time.Time     `json:"data_atualizacao"`
	Fonte         string        `json:"fonte"`
	UnidadeMedida string        `json:"unidade_medida"`
	Data          []DataPobreza `json:"data"`
}

type DataPobrezaConsolidado struct {
	Referencia                 string  `json:"referencia" csv:"referencia"`
	Periodo                    int64   `json:"periodo" csv:"periodo"`
	Pobreza                    int64   `json:"pobreza" csv:"pobreza"`
	ExtremaPobreza             int64   `json:"extrema_pobreza" csv:"extrema_pobreza"`
	Total                      int64   `json:"total" csv:"total"`
	Populacao                  int64   `json:"populacao_estimada" csv:"populacao_estimada"`
	PorcentagemPobreza         float64 `json:"porcentagem_pobreza" csv:"porcentagem_pobreza"`
	PorcentagemExtremaPobreza  float64 `json:"porcentagem_extrema_pobreza" csv:"porcentagem_extrema_pobreza"`
	PorcentagemVulnerabilidade float64 `json:"porcentagem_vulnerabilidade" csv:"porcentagem_vulnerabilidade"`
	FamiliasPobreza            int64   `json:"familias_pobreza" csv:"familias_pobreza"`
	FamiliasExtremaPobreza     int64   `json:"familias_extrema_pobreza" csv:"familias_extrema_pobreza"`
	FamiliasVulnerabilidade    int64   `json:"familias_vulnerabilidade" csv:"familias_vulnerabilidade"`
	IndigenasPobreza           int64   `json:"indigenas_pobreza" csv:"indigenas_pobreza"`
	IndigenasExtremaPobreza    int64   `json:"indigenas_extrema_pobreza" csv:"indigenas_extrema_pobreza"`
	IndigenasVulnerabilidade   int64   `json:"indigenas_vulnerabilidade" csv:"indigenas_vulnerabilidade"`
	QuilombolasPobreza         int64   `json:"quilombolas_pobreza" csv:"quilombolas_pobreza"`
	QuilombolasExtremaPobreza  int64   `json:"quilombolas_extrema_pobreza" csv:"quilombolas_extrema_pobreza"`
	QuilombolasVulnerabilidade int64   `json:"quilombolas_vulnerabilidade" csv:"quilombolas_vulnerabilidade"`
	CiganosPobreza             int64   `json:"ciganos_pobreza" csv:"ciganos_pobreza"`
	CiganosExtremaPobreza      int64   `json:"ciganos_extrema_pobreza" csv:"ciganos_extrema_pobreza"`
	CiganosVulnerabilidade     int64   `json:"ciganos_vulnerabilidade" csv:"ciganos_vulnerabilidade"`
}

type PobrezaConsolidado struct {
	Atualizacao   time.Time                `json:"data_atualizacao"`
	Fonte         string                   `json:"fonte"`
	UnidadeMedida string                   `json:"unidade_medida"`
	Data          []DataPobrezaConsolidado `json:"data"`
}

func RunnerConsolidacaoPobreza() {
	l := logger.Instance()

	consolidado := make(map[string]DataPobrezaConsolidado)

	runnerName := "Consolidação Pobreza Cadastro Unico"
	pobrezaFile := "./data/sociais/pobreza_cadastro_unico.json"
	extremaPobrezaFile := "./data/sociais/extrema_pobreza_cadastro_unico.json"
	pobrezaExtremaPobrezaFile := "./data/sociais/pobreza_extrema_pobreza_cadastro_unico.json"

	familiasPobrezaFile := "./data/sociais/familias_pobreza_cadastro_unico.json"
	familiasExtremaPobrezaFile := "./data/sociais/familias_extrema_pobreza_cadastro_unico.json"

	indigenasPobrezaFile := "./data/sociais/indigenas_pobreza_cadastro_unico.json"
	indigenasExtremaPobrezaFile := "./data/sociais/indigenas_extrema_pobreza_cadastro_unico.json"

	quilombolasPobrezaFile := "./data/sociais/quilombolas_pobreza_cadastro_unico.json"
	quilombolasExtremaPobrezaFile := "./data/sociais/quilombolas_extrema_pobreza_cadastro_unico.json"

	ciganosPobrezaFile := "./data/sociais/ciganos_pobreza_cadastro_unico.json"
	ciganosExtremaPobrezaFile := "./data/sociais/ciganos_extrema_pobreza_cadastro_unico.json"

	estimativaPopulacaoFile := "./data/sociais/estimativa_populacional.json"

	file_path := "./data/sociais/indices_pobreza_consolidado.json"
	fileNameOutputCSV := "./data/sociais/indices_pobreza_consolidado.csv"

	file_path_anual := "./data/sociais/indices_pobreza_consolidado_anual.json"
	fileNameAnualOutputCSV := "./data/sociais/indices_pobreza_consolidado_anual.csv"

	s3KeyCSV := "sociais/indices_pobreza_consolidado.csv"
	s3KeyJSON := "sociais/indices_pobreza_consolidado.json"

	s3KeyCSVAnual := "sociais/indices_pobreza_consolidado_anual.csv"
	s3KeyJSONAnual := "sociais/indices_pobreza_consolidado_anual.json"

	fonte := "https://aplicacoes.cidadania.gov.br"

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Processo de Consolidação de dados")

	indice := PobrezaConsolidado{}
	now := time.Now()
	indice.Atualizacao = now
	indice.Fonte = fonte

	l.Info().
		Str("Runner", runnerName).
		Msg("Abrindo os arquivos gerados pelos runners")

	// Pobreza
	dataPobreza := Pobreza{}
	filePobreza, err := ioutil.ReadFile(pobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(filePobreza), &dataPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Pobreza para o Dataset Consolidado")

	for _, k := range dataPobreza.Data {
		splitData := strings.Split(k.Referencia, "/")
		dataRaw := fmt.Sprintf("%v%v", splitData[1], splitData[0])
		periodo, _ := strconv.ParseInt(dataRaw, 10, 64)

		item := DataPobrezaConsolidado{
			Periodo:    periodo,
			Referencia: k.Referencia,
			Pobreza:    k.Total,
		}

		consolidado[k.Referencia] = item

	}

	// Extrema Pobreza
	dataExtremaPobreza := Pobreza{}
	fileExtremaPobreza, err := ioutil.ReadFile(extremaPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", extremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileExtremaPobreza), &dataExtremaPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", extremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Pobreza para o Dataset Consolidado")

	for _, k := range dataExtremaPobreza.Data {
		item := consolidado[k.Referencia]
		item.ExtremaPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Pobreza / Extrema Pobreza
	dataPobrezaExtremaPobreza := Pobreza{}
	filePobrezaExtremaPobreza, err := ioutil.ReadFile(pobrezaExtremaPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pobrezaExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(filePobrezaExtremaPobreza), &dataPobrezaExtremaPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", pobrezaExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Pobreza para o Dataset Consolidado")

	for _, k := range dataPobrezaExtremaPobreza.Data {
		item := consolidado[k.Referencia]
		item.Total = k.Total
		consolidado[k.Referencia] = item
	}

	// Familias Pobreza
	dataFamiliasPobreza := Pobreza{}
	fileFamiliasPobreza, err := ioutil.ReadFile(familiasPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileFamiliasPobreza), &dataFamiliasPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Pobreza para o Dataset Consolidado")

	for _, k := range dataFamiliasPobreza.Data {
		item := consolidado[k.Referencia]
		item.FamiliasPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Familias Extrema Pobreza
	dataFamiliasExtremaPobreza := Pobreza{}
	fileFamiliasExtremaPobrezaPobreza, err := ioutil.ReadFile(familiasExtremaPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileFamiliasExtremaPobrezaPobreza), &dataFamiliasExtremaPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataFamiliasExtremaPobreza.Data {
		item := consolidado[k.Referencia]
		item.FamiliasExtremaPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Indigenas Pobreza
	dataIndigenasPobreza := Pobreza{}
	fileIndigenasPobrezaPobreza, err := ioutil.ReadFile(indigenasPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIndigenasPobrezaPobreza), &dataIndigenasPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataIndigenasPobreza.Data {
		item := consolidado[k.Referencia]
		item.IndigenasPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Indigenas Pobreza
	dataIndigenasExtremaPobreza := Pobreza{}
	fileIndigenasExtremaPobrezaPobreza, err := ioutil.ReadFile(indigenasExtremaPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileIndigenasExtremaPobrezaPobreza), &dataIndigenasExtremaPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataIndigenasExtremaPobreza.Data {
		item := consolidado[k.Referencia]
		item.IndigenasExtremaPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Quilombolas Pobreza
	dataQuilombolasPobreza := Pobreza{}
	fileQuilombolasPobrezaPobreza, err := ioutil.ReadFile(quilombolasPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileQuilombolasPobrezaPobreza), &dataQuilombolasPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataQuilombolasPobreza.Data {
		item := consolidado[k.Referencia]
		item.QuilombolasPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Quilombolas Pobreza
	dataQuilombolasExtremaPobreza := Pobreza{}
	fileQuilombolasExtremaPobrezaPobreza, err := ioutil.ReadFile(quilombolasExtremaPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileQuilombolasExtremaPobrezaPobreza), &dataQuilombolasExtremaPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataQuilombolasExtremaPobreza.Data {
		item := consolidado[k.Referencia]
		item.QuilombolasExtremaPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Ciganos Pobreza
	dataCiganosPobreza := Pobreza{}
	fileCiganosPobrezaPobreza, err := ioutil.ReadFile(ciganosPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ciganosExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileCiganosPobrezaPobreza), &dataCiganosPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", ciganosExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataCiganosPobreza.Data {
		item := consolidado[k.Referencia]
		item.CiganosPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Ciganos Pobreza
	dataCiganosExtremaPobreza := Pobreza{}
	fileCiganosExtremaPobrezaPobreza, err := ioutil.ReadFile(ciganosExtremaPobrezaFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileCiganosExtremaPobrezaPobreza), &dataCiganosExtremaPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", familiasExtremaPobrezaFile).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataCiganosExtremaPobreza.Data {
		item := consolidado[k.Referencia]
		item.CiganosExtremaPobreza = k.Total
		consolidado[k.Referencia] = item
	}

	// Estimativa Populacao
	dataPopulacao := EstimativaPopulacional{}
	filePopulacao, err := ioutil.ReadFile(estimativaPopulacaoFile)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", estimativaPopulacaoFile).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(filePopulacao), &dataPopulacao)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", estimativaPopulacaoFile).
			Msg("converter para struct")
	}

	// Consolidando dados populacionais
	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Estimativa Populacional para o Dataset Consolidado")

	for _, k := range dataPopulacao.Data {
		for i, c := range consolidado {
			if c.Pobreza != 0 {
				indiceAnual := i[3:7]
				if indiceAnual == k.Referencia {
					item := consolidado[i]
					item.Populacao = k.Total
					consolidado[i] = item
				}
			}
		}
	}

	// Calculando Porcentagens
	l.Info().
		Str("Runner", runnerName).
		Msg("Calculando a porcentagem de mizéria")

	for i, _ := range consolidado {
		item := consolidado[i]

		item.PorcentagemPobreza = float64(item.Pobreza) / float64(item.Populacao)
		item.PorcentagemExtremaPobreza = float64(item.ExtremaPobreza) / float64(item.Populacao)
		item.PorcentagemVulnerabilidade = float64(item.Total) / float64(item.Populacao)

		item.PorcentagemPobreza = math.Round(item.PorcentagemPobreza*100) / 100
		item.PorcentagemExtremaPobreza = math.Round(item.PorcentagemExtremaPobreza*100) / 100
		item.PorcentagemVulnerabilidade = math.Round(item.PorcentagemVulnerabilidade*100) / 100

		item.FamiliasVulnerabilidade = item.FamiliasExtremaPobreza + item.FamiliasPobreza
		item.IndigenasVulnerabilidade = item.IndigenasExtremaPobreza + item.IndigenasPobreza
		item.QuilombolasVulnerabilidade = item.QuilombolasExtremaPobreza + item.QuilombolasPobreza
		item.CiganosVulnerabilidade = item.CiganosExtremaPobreza + item.CiganosPobreza
		consolidado[i] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Porcentagens calculadas")

	for _, k := range consolidado {
		indice.Data = append(indice.Data, k)
	}

	// Sort do data
	sort.Slice(indice.Data, func(i, j int) bool {
		return indice.Data[i].Periodo < indice.Data[j].Periodo
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(indice)
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

	csvOutput, err := gocsv.MarshalString(&indice.Data)
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

	anual := indice
	anual.Data = []DataPobrezaConsolidado{}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Dataset em CSV Criado")

	for _, k := range indice.Data {
		if strings.HasPrefix(k.Referencia, "12") {
			k.Referencia = k.Referencia[3:7]
			anual.Data = append(anual.Data, k)
		}
	}

	// Fix do Ultimo Item do Array
	lItem := indice.Data[len(indice.Data)-1]
	if !strings.HasPrefix(lItem.Referencia, "12") {
		lItem.Referencia = lItem.Referencia[3:7]
		anual.Data = append(anual.Data, lItem)
	}

	a, err := json.Marshal(anual)
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

	fa, err := os.Create(file_path_anual)
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

	_, err = fa.WriteString(string(a))

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	// Convertendo em CSV
	csvFileAnual, err := os.OpenFile(fileNameAnualOutputCSV, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameAnualOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao criar o dataset em CSV")
	}
	defer csvFileAnual.Close()

	csvOutputAnual, err := gocsv.MarshalString(&anual.Data)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameAnualOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao escrever o dataset em CSV")
	}

	_, err = csvFileAnual.WriteString(string(csvOutputAnual))
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameAnualOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro para escrever os dados no arquivo")
	}

	// Upload Consolidado
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

	// Upload Consolidado Anual
	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameAnualOutputCSV).
		Str("S3Key", s3KeyCSVAnual).
		Msg("Fazendo Upload para o S3")

	err = upload.S3(fileNameAnualOutputCSV, s3KeyCSVAnual)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameAnualOutputCSV).
			Str("S3Key", s3KeyCSVAnual).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path_anual).
		Str("S3Key", s3KeyJSONAnual).
		Msg("Fazendo Upload para o S3")

	err = upload.S3(file_path_anual, s3KeyJSONAnual)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path_anual).
			Str("S3Key", s3KeyJSONAnual).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}
	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path_anual).
		Msg("Finalizado")
}
