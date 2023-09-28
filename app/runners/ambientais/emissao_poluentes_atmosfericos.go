package ambientais

import (
	"crawlers/pkg/logger"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocarina/gocsv"
)

type PoluentesAtmosfericosEmpresasResponse struct {
	Data []struct {
		Estado               string `json:"estado"`
		Detalhe              string `json:"detalhe"`
		Ano                  string `json:"ano"`
		SituacaoCadastral    string `json:"situacaoCadastral"`
		Municipio            string `json:"municipio"`
		CodigoDetalhe        int    `json:"codigoDetalhe"`
		CodigoCategoria      int    `json:"codigoCategoria"`
		Cnpj                 string `json:"cnpj"`
		PoluenteEmitido      string `json:"poluenteEmitido"`
		CategoriaAtividade   string `json:"categoriaAtividade"`
		MetodologiaUtilizada string `json:"metodologiaUtilizada"`
		RazaoSocial          string `json:"razaoSocial"`
		Quantidade           string `json:"quantidade"`
	} `json:"data"`
}

type DataPoluentesAtmosfericos struct {
	Referencia        string  `json:"referencia" csv:"referencia"`
	RazaoSocial       string  `json:"razao_social" csv:"razao_social"`
	CNPJ              string  `json:"cnpj" csv:"cnpj"`
	SituacaoCadastral string  `json:"situacao_cadastral" csv:"situacao_cadastral"`
	Estado            string  `json:"estado" csv:"estado"`
	Municipio         string  `json:"municipio" csv:"municipio"`
	Categoria         string  `json:"categoria" csv:"categoria"`
	Poluente          string  `json:"poluente_emitido" csv:"poluente_emitido"`
	Quantidade        float64 `json:"quantidade" csv:"quantidade"`
}

type PoluentesAtmosfericosEmpresas struct {
	Atualizacao time.Time                   `json:"data_atualizacao"`
	Fonte       string                      `json:"fonte"`
	Data        []DataPoluentesAtmosfericos `json:"data"`
}

func RunnerPoluentesAtmosfericosEmpresas() {
	runnerName := "Poluentes Atmosféricos - Empresas"
	domain := "dadosabertos.ibama.gov.br"
	url := "https://dadosabertos.ibama.gov.br/dados/RAPP/emissoesPoluentesAtmosfericos/relatorio.json"
	file_path := "./data/ambientais/empresas_emissao_poluentes.json"
	fileNameOutputCSV := "./data/ambientais/empresas_emissao_poluentes.csv"

	s3KeyCSV := "ambientais/empresas_emissao_poluentes.csv"
	s3KeyJSON := "ambientais/empresas_emissao_poluentes.csv"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	index := PoluentesAtmosfericosEmpresas{}
	now := time.Now()
	index.Atualizacao = now
	index.Fonte = domain

	var response PoluentesAtmosfericosEmpresasResponse

	transCfg := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: transCfg}

	res, err := client.Get(url)
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

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	for _, v := range response.Data {
		valor_raw := strings.Replace(v.Quantidade, ",", "", -1)
		valor, err := strconv.ParseFloat(strings.TrimSpace(valor_raw), 64)

		razao_social := strings.Replace(v.RazaoSocial, ",", "", -1)
		razao_social = strings.Replace(razao_social, ".", "", -1)

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", v.Quantidade).
				Msg("Erro ao converter o valor para Float64")
		}

		item := DataPoluentesAtmosfericos{
			Referencia:        v.Ano,
			Estado:            v.Estado,
			Municipio:         v.Municipio,
			RazaoSocial:       razao_social,
			CNPJ:              v.Cnpj,
			SituacaoCadastral: v.SituacaoCadastral,
			Categoria:         v.CategoriaAtividade,
			Poluente:          v.PoluenteEmitido,
			Quantidade:        valor,
		}

		index.Data = append(index.Data, item)
	}

	// Sort do data
	sort.Slice(index.Data, func(i, j int) bool {
		return index.Data[i].Referencia < index.Data[j].Referencia
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(index)
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

	csvOutput, err := gocsv.MarshalString(&index.Data)
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

	// err = upload.S3(fileNameOutputCSV, s3KeyCSV)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("S3Key", s3KeyCSV).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	// err = upload.S3(file_path, s3KeyJSON)

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

	var wg sync.WaitGroup

	l.Info().
		Msg("Iniciando o Runner de Poluentes Municipios e de Estados")

	wg.Add(1)
	go func() {
		defer wg.Done()
		RunnerPoluentesAtmosfericosMunicipios(index)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		RunnerPoluentesAtmosfericosMunicipiosResumido(index)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		RunnerPoluentesAtmosfericosEstados(index)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		RunnerPoluentesAtmosfericosEstadosResumido(index)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		RunnerPoluentesAtmosfericosAnualConsolidado(index)
	}()

	wg.Wait()

}
