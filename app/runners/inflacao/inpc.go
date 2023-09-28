package inflacao

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/gocarina/gocsv"
)

type INPCResponse []struct {
	ID         string `json:"id"`
	Variavel   string `json:"variavel"`
	Unidade    string `json:"unidade"`
	Resultados []struct {
		Classificacoes []interface{} `json:"classificacoes"`
		Series         []struct {
			Localidade struct {
				ID    string `json:"id"`
				Nivel struct {
					ID   string `json:"id"`
					Nome string `json:"nome"`
				} `json:"nivel"`
				Nome string `json:"nome"`
			} `json:"localidade"`
			Serie struct {
				Num197912 string `json:"197912"`
				Num198001 string `json:"198001"`
				Num198002 string `json:"198002"`
				Num198003 string `json:"198003"`
				Num198004 string `json:"198004"`
				Num198005 string `json:"198005"`
				Num198006 string `json:"198006"`
				Num198007 string `json:"198007"`
				Num198008 string `json:"198008"`
				Num198009 string `json:"198009"`
				Num198010 string `json:"198010"`
				Num198011 string `json:"198011"`
				Num198012 string `json:"198012"`
				Num198101 string `json:"198101"`
				Num198102 string `json:"198102"`
				Num198103 string `json:"198103"`
				Num198104 string `json:"198104"`
				Num198105 string `json:"198105"`
				Num198106 string `json:"198106"`
				Num198107 string `json:"198107"`
				Num198108 string `json:"198108"`
				Num198109 string `json:"198109"`
				Num198110 string `json:"198110"`
				Num198111 string `json:"198111"`
				Num198112 string `json:"198112"`
				Num198201 string `json:"198201"`
				Num198202 string `json:"198202"`
				Num198203 string `json:"198203"`
				Num198204 string `json:"198204"`
				Num198205 string `json:"198205"`
				Num198206 string `json:"198206"`
				Num198207 string `json:"198207"`
				Num198208 string `json:"198208"`
				Num198209 string `json:"198209"`
				Num198210 string `json:"198210"`
				Num198211 string `json:"198211"`
				Num198212 string `json:"198212"`
				Num198301 string `json:"198301"`
				Num198302 string `json:"198302"`
				Num198303 string `json:"198303"`
				Num198304 string `json:"198304"`
				Num198305 string `json:"198305"`
				Num198306 string `json:"198306"`
				Num198307 string `json:"198307"`
				Num198308 string `json:"198308"`
				Num198309 string `json:"198309"`
				Num198310 string `json:"198310"`
				Num198311 string `json:"198311"`
				Num198312 string `json:"198312"`
				Num198401 string `json:"198401"`
				Num198402 string `json:"198402"`
				Num198403 string `json:"198403"`
				Num198404 string `json:"198404"`
				Num198405 string `json:"198405"`
				Num198406 string `json:"198406"`
				Num198407 string `json:"198407"`
				Num198408 string `json:"198408"`
				Num198409 string `json:"198409"`
				Num198410 string `json:"198410"`
				Num198411 string `json:"198411"`
				Num198412 string `json:"198412"`
				Num198501 string `json:"198501"`
				Num198502 string `json:"198502"`
				Num198503 string `json:"198503"`
				Num198504 string `json:"198504"`
				Num198505 string `json:"198505"`
				Num198506 string `json:"198506"`
				Num198507 string `json:"198507"`
				Num198508 string `json:"198508"`
				Num198509 string `json:"198509"`
				Num198510 string `json:"198510"`
				Num198511 string `json:"198511"`
				Num198512 string `json:"198512"`
				Num198601 string `json:"198601"`
				Num198602 string `json:"198602"`
				Num198603 string `json:"198603"`
				Num198604 string `json:"198604"`
				Num198605 string `json:"198605"`
				Num198606 string `json:"198606"`
				Num198607 string `json:"198607"`
				Num198608 string `json:"198608"`
				Num198609 string `json:"198609"`
				Num198610 string `json:"198610"`
				Num198611 string `json:"198611"`
				Num198612 string `json:"198612"`
				Num198701 string `json:"198701"`
				Num198702 string `json:"198702"`
				Num198703 string `json:"198703"`
				Num198704 string `json:"198704"`
				Num198705 string `json:"198705"`
				Num198706 string `json:"198706"`
				Num198707 string `json:"198707"`
				Num198708 string `json:"198708"`
				Num198709 string `json:"198709"`
				Num198710 string `json:"198710"`
				Num198711 string `json:"198711"`
				Num198712 string `json:"198712"`
				Num198801 string `json:"198801"`
				Num198802 string `json:"198802"`
				Num198803 string `json:"198803"`
				Num198804 string `json:"198804"`
				Num198805 string `json:"198805"`
				Num198806 string `json:"198806"`
				Num198807 string `json:"198807"`
				Num198808 string `json:"198808"`
				Num198809 string `json:"198809"`
				Num198810 string `json:"198810"`
				Num198811 string `json:"198811"`
				Num198812 string `json:"198812"`
				Num198901 string `json:"198901"`
				Num198902 string `json:"198902"`
				Num198903 string `json:"198903"`
				Num198904 string `json:"198904"`
				Num198905 string `json:"198905"`
				Num198906 string `json:"198906"`
				Num198907 string `json:"198907"`
				Num198908 string `json:"198908"`
				Num198909 string `json:"198909"`
				Num198910 string `json:"198910"`
				Num198911 string `json:"198911"`
				Num198912 string `json:"198912"`
				Num199001 string `json:"199001"`
				Num199002 string `json:"199002"`
				Num199003 string `json:"199003"`
				Num199004 string `json:"199004"`
				Num199005 string `json:"199005"`
				Num199006 string `json:"199006"`
				Num199007 string `json:"199007"`
				Num199008 string `json:"199008"`
				Num199009 string `json:"199009"`
				Num199010 string `json:"199010"`
				Num199011 string `json:"199011"`
				Num199012 string `json:"199012"`
				Num199101 string `json:"199101"`
				Num199102 string `json:"199102"`
				Num199103 string `json:"199103"`
				Num199104 string `json:"199104"`
				Num199105 string `json:"199105"`
				Num199106 string `json:"199106"`
				Num199107 string `json:"199107"`
				Num199108 string `json:"199108"`
				Num199109 string `json:"199109"`
				Num199110 string `json:"199110"`
				Num199111 string `json:"199111"`
				Num199112 string `json:"199112"`
				Num199201 string `json:"199201"`
				Num199202 string `json:"199202"`
				Num199203 string `json:"199203"`
				Num199204 string `json:"199204"`
				Num199205 string `json:"199205"`
				Num199206 string `json:"199206"`
				Num199207 string `json:"199207"`
				Num199208 string `json:"199208"`
				Num199209 string `json:"199209"`
				Num199210 string `json:"199210"`
				Num199211 string `json:"199211"`
				Num199212 string `json:"199212"`
				Num199301 string `json:"199301"`
				Num199302 string `json:"199302"`
				Num199303 string `json:"199303"`
				Num199304 string `json:"199304"`
				Num199305 string `json:"199305"`
				Num199306 string `json:"199306"`
				Num199307 string `json:"199307"`
				Num199308 string `json:"199308"`
				Num199309 string `json:"199309"`
				Num199310 string `json:"199310"`
				Num199311 string `json:"199311"`
				Num199312 string `json:"199312"`
				Num199401 string `json:"199401"`
				Num199402 string `json:"199402"`
				Num199403 string `json:"199403"`
				Num199404 string `json:"199404"`
				Num199405 string `json:"199405"`
				Num199406 string `json:"199406"`
				Num199407 string `json:"199407"`
				Num199408 string `json:"199408"`
				Num199409 string `json:"199409"`
				Num199410 string `json:"199410"`
				Num199411 string `json:"199411"`
				Num199412 string `json:"199412"`
				Num199501 string `json:"199501"`
				Num199502 string `json:"199502"`
				Num199503 string `json:"199503"`
				Num199504 string `json:"199504"`
				Num199505 string `json:"199505"`
				Num199506 string `json:"199506"`
				Num199507 string `json:"199507"`
				Num199508 string `json:"199508"`
				Num199509 string `json:"199509"`
				Num199510 string `json:"199510"`
				Num199511 string `json:"199511"`
				Num199512 string `json:"199512"`
				Num199601 string `json:"199601"`
				Num199602 string `json:"199602"`
				Num199603 string `json:"199603"`
				Num199604 string `json:"199604"`
				Num199605 string `json:"199605"`
				Num199606 string `json:"199606"`
				Num199607 string `json:"199607"`
				Num199608 string `json:"199608"`
				Num199609 string `json:"199609"`
				Num199610 string `json:"199610"`
				Num199611 string `json:"199611"`
				Num199612 string `json:"199612"`
				Num199701 string `json:"199701"`
				Num199702 string `json:"199702"`
				Num199703 string `json:"199703"`
				Num199704 string `json:"199704"`
				Num199705 string `json:"199705"`
				Num199706 string `json:"199706"`
				Num199707 string `json:"199707"`
				Num199708 string `json:"199708"`
				Num199709 string `json:"199709"`
				Num199710 string `json:"199710"`
				Num199711 string `json:"199711"`
				Num199712 string `json:"199712"`
				Num199801 string `json:"199801"`
				Num199802 string `json:"199802"`
				Num199803 string `json:"199803"`
				Num199804 string `json:"199804"`
				Num199805 string `json:"199805"`
				Num199806 string `json:"199806"`
				Num199807 string `json:"199807"`
				Num199808 string `json:"199808"`
				Num199809 string `json:"199809"`
				Num199810 string `json:"199810"`
				Num199811 string `json:"199811"`
				Num199812 string `json:"199812"`
				Num199901 string `json:"199901"`
				Num199902 string `json:"199902"`
				Num199903 string `json:"199903"`
				Num199904 string `json:"199904"`
				Num199905 string `json:"199905"`
				Num199906 string `json:"199906"`
				Num199907 string `json:"199907"`
				Num199908 string `json:"199908"`
				Num199909 string `json:"199909"`
				Num199910 string `json:"199910"`
				Num199911 string `json:"199911"`
				Num199912 string `json:"199912"`
				Num200001 string `json:"200001"`
				Num200002 string `json:"200002"`
				Num200003 string `json:"200003"`
				Num200004 string `json:"200004"`
				Num200005 string `json:"200005"`
				Num200006 string `json:"200006"`
				Num200007 string `json:"200007"`
				Num200008 string `json:"200008"`
				Num200009 string `json:"200009"`
				Num200010 string `json:"200010"`
				Num200011 string `json:"200011"`
				Num200012 string `json:"200012"`
				Num200101 string `json:"200101"`
				Num200102 string `json:"200102"`
				Num200103 string `json:"200103"`
				Num200104 string `json:"200104"`
				Num200105 string `json:"200105"`
				Num200106 string `json:"200106"`
				Num200107 string `json:"200107"`
				Num200108 string `json:"200108"`
				Num200109 string `json:"200109"`
				Num200110 string `json:"200110"`
				Num200111 string `json:"200111"`
				Num200112 string `json:"200112"`
				Num200201 string `json:"200201"`
				Num200202 string `json:"200202"`
				Num200203 string `json:"200203"`
				Num200204 string `json:"200204"`
				Num200205 string `json:"200205"`
				Num200206 string `json:"200206"`
				Num200207 string `json:"200207"`
				Num200208 string `json:"200208"`
				Num200209 string `json:"200209"`
				Num200210 string `json:"200210"`
				Num200211 string `json:"200211"`
				Num200212 string `json:"200212"`
				Num200301 string `json:"200301"`
				Num200302 string `json:"200302"`
				Num200303 string `json:"200303"`
				Num200304 string `json:"200304"`
				Num200305 string `json:"200305"`
				Num200306 string `json:"200306"`
				Num200307 string `json:"200307"`
				Num200308 string `json:"200308"`
				Num200309 string `json:"200309"`
				Num200310 string `json:"200310"`
				Num200311 string `json:"200311"`
				Num200312 string `json:"200312"`
				Num200401 string `json:"200401"`
				Num200402 string `json:"200402"`
				Num200403 string `json:"200403"`
				Num200404 string `json:"200404"`
				Num200405 string `json:"200405"`
				Num200406 string `json:"200406"`
				Num200407 string `json:"200407"`
				Num200408 string `json:"200408"`
				Num200409 string `json:"200409"`
				Num200410 string `json:"200410"`
				Num200411 string `json:"200411"`
				Num200412 string `json:"200412"`
				Num200501 string `json:"200501"`
				Num200502 string `json:"200502"`
				Num200503 string `json:"200503"`
				Num200504 string `json:"200504"`
				Num200505 string `json:"200505"`
				Num200506 string `json:"200506"`
				Num200507 string `json:"200507"`
				Num200508 string `json:"200508"`
				Num200509 string `json:"200509"`
				Num200510 string `json:"200510"`
				Num200511 string `json:"200511"`
				Num200512 string `json:"200512"`
				Num200601 string `json:"200601"`
				Num200602 string `json:"200602"`
				Num200603 string `json:"200603"`
				Num200604 string `json:"200604"`
				Num200605 string `json:"200605"`
				Num200606 string `json:"200606"`
				Num200607 string `json:"200607"`
				Num200608 string `json:"200608"`
				Num200609 string `json:"200609"`
				Num200610 string `json:"200610"`
				Num200611 string `json:"200611"`
				Num200612 string `json:"200612"`
				Num200701 string `json:"200701"`
				Num200702 string `json:"200702"`
				Num200703 string `json:"200703"`
				Num200704 string `json:"200704"`
				Num200705 string `json:"200705"`
				Num200706 string `json:"200706"`
				Num200707 string `json:"200707"`
				Num200708 string `json:"200708"`
				Num200709 string `json:"200709"`
				Num200710 string `json:"200710"`
				Num200711 string `json:"200711"`
				Num200712 string `json:"200712"`
				Num200801 string `json:"200801"`
				Num200802 string `json:"200802"`
				Num200803 string `json:"200803"`
				Num200804 string `json:"200804"`
				Num200805 string `json:"200805"`
				Num200806 string `json:"200806"`
				Num200807 string `json:"200807"`
				Num200808 string `json:"200808"`
				Num200809 string `json:"200809"`
				Num200810 string `json:"200810"`
				Num200811 string `json:"200811"`
				Num200812 string `json:"200812"`
				Num200901 string `json:"200901"`
				Num200902 string `json:"200902"`
				Num200903 string `json:"200903"`
				Num200904 string `json:"200904"`
				Num200905 string `json:"200905"`
				Num200906 string `json:"200906"`
				Num200907 string `json:"200907"`
				Num200908 string `json:"200908"`
				Num200909 string `json:"200909"`
				Num200910 string `json:"200910"`
				Num200911 string `json:"200911"`
				Num200912 string `json:"200912"`
				Num201001 string `json:"201001"`
				Num201002 string `json:"201002"`
				Num201003 string `json:"201003"`
				Num201004 string `json:"201004"`
				Num201005 string `json:"201005"`
				Num201006 string `json:"201006"`
				Num201007 string `json:"201007"`
				Num201008 string `json:"201008"`
				Num201009 string `json:"201009"`
				Num201010 string `json:"201010"`
				Num201011 string `json:"201011"`
				Num201012 string `json:"201012"`
				Num201101 string `json:"201101"`
				Num201102 string `json:"201102"`
				Num201103 string `json:"201103"`
				Num201104 string `json:"201104"`
				Num201105 string `json:"201105"`
				Num201106 string `json:"201106"`
				Num201107 string `json:"201107"`
				Num201108 string `json:"201108"`
				Num201109 string `json:"201109"`
				Num201110 string `json:"201110"`
				Num201111 string `json:"201111"`
				Num201112 string `json:"201112"`
				Num201201 string `json:"201201"`
				Num201202 string `json:"201202"`
				Num201203 string `json:"201203"`
				Num201204 string `json:"201204"`
				Num201205 string `json:"201205"`
				Num201206 string `json:"201206"`
				Num201207 string `json:"201207"`
				Num201208 string `json:"201208"`
				Num201209 string `json:"201209"`
				Num201210 string `json:"201210"`
				Num201211 string `json:"201211"`
				Num201212 string `json:"201212"`
				Num201301 string `json:"201301"`
				Num201302 string `json:"201302"`
				Num201303 string `json:"201303"`
				Num201304 string `json:"201304"`
				Num201305 string `json:"201305"`
				Num201306 string `json:"201306"`
				Num201307 string `json:"201307"`
				Num201308 string `json:"201308"`
				Num201309 string `json:"201309"`
				Num201310 string `json:"201310"`
				Num201311 string `json:"201311"`
				Num201312 string `json:"201312"`
				Num201401 string `json:"201401"`
				Num201402 string `json:"201402"`
				Num201403 string `json:"201403"`
				Num201404 string `json:"201404"`
				Num201405 string `json:"201405"`
				Num201406 string `json:"201406"`
				Num201407 string `json:"201407"`
				Num201408 string `json:"201408"`
				Num201409 string `json:"201409"`
				Num201410 string `json:"201410"`
				Num201411 string `json:"201411"`
				Num201412 string `json:"201412"`
				Num201501 string `json:"201501"`
				Num201502 string `json:"201502"`
				Num201503 string `json:"201503"`
				Num201504 string `json:"201504"`
				Num201505 string `json:"201505"`
				Num201506 string `json:"201506"`
				Num201507 string `json:"201507"`
				Num201508 string `json:"201508"`
				Num201509 string `json:"201509"`
				Num201510 string `json:"201510"`
				Num201511 string `json:"201511"`
				Num201512 string `json:"201512"`
				Num201601 string `json:"201601"`
				Num201602 string `json:"201602"`
				Num201603 string `json:"201603"`
				Num201604 string `json:"201604"`
				Num201605 string `json:"201605"`
				Num201606 string `json:"201606"`
				Num201607 string `json:"201607"`
				Num201608 string `json:"201608"`
				Num201609 string `json:"201609"`
				Num201610 string `json:"201610"`
				Num201611 string `json:"201611"`
				Num201612 string `json:"201612"`
				Num201701 string `json:"201701"`
				Num201702 string `json:"201702"`
				Num201703 string `json:"201703"`
				Num201704 string `json:"201704"`
				Num201705 string `json:"201705"`
				Num201706 string `json:"201706"`
				Num201707 string `json:"201707"`
				Num201708 string `json:"201708"`
				Num201709 string `json:"201709"`
				Num201710 string `json:"201710"`
				Num201711 string `json:"201711"`
				Num201712 string `json:"201712"`
				Num201801 string `json:"201801"`
				Num201802 string `json:"201802"`
				Num201803 string `json:"201803"`
				Num201804 string `json:"201804"`
				Num201805 string `json:"201805"`
				Num201806 string `json:"201806"`
				Num201807 string `json:"201807"`
				Num201808 string `json:"201808"`
				Num201809 string `json:"201809"`
				Num201810 string `json:"201810"`
				Num201811 string `json:"201811"`
				Num201812 string `json:"201812"`
				Num201901 string `json:"201901"`
				Num201902 string `json:"201902"`
				Num201903 string `json:"201903"`
				Num201904 string `json:"201904"`
				Num201905 string `json:"201905"`
				Num201906 string `json:"201906"`
				Num201907 string `json:"201907"`
				Num201908 string `json:"201908"`
				Num201909 string `json:"201909"`
				Num201910 string `json:"201910"`
				Num201911 string `json:"201911"`
				Num201912 string `json:"201912"`
				Num202001 string `json:"202001"`
				Num202002 string `json:"202002"`
				Num202003 string `json:"202003"`
				Num202004 string `json:"202004"`
				Num202005 string `json:"202005"`
				Num202006 string `json:"202006"`
				Num202007 string `json:"202007"`
				Num202008 string `json:"202008"`
				Num202009 string `json:"202009"`
				Num202010 string `json:"202010"`
				Num202011 string `json:"202011"`
				Num202012 string `json:"202012"`
				Num202101 string `json:"202101"`
				Num202102 string `json:"202102"`
				Num202103 string `json:"202103"`
				Num202104 string `json:"202104"`
				Num202105 string `json:"202105"`
				Num202106 string `json:"202106"`
				Num202107 string `json:"202107"`
				Num202108 string `json:"202108"`
				Num202109 string `json:"202109"`
				Num202110 string `json:"202110"`
				Num202111 string `json:"202111"`
				Num202112 string `json:"202112"`
				Num202201 string `json:"202201"`
				Num202202 string `json:"202202"`
				Num202203 string `json:"202203"`
				Num202204 string `json:"202204"`
				Num202205 string `json:"202205"`
				Num202206 string `json:"202206"`
				Num202207 string `json:"202207"`
				Num202208 string `json:"202208"`
				Num202209 string `json:"202209"`
				Num202210 string `json:"202210"`
				Num202211 string `json:"202211"`
				Num202212 string `json:"202212"`
				Num202301 string `json:"202301"`
				Num202302 string `json:"202302"`
				Num202303 string `json:"202303"`
				Num202304 string `json:"202304"`
				Num202305 string `json:"202305"`
				Num202306 string `json:"202306"`
				Num202307 string `json:"202307"`
				Num202308 string `json:"202308"`
				Num202309 string `json:"202309"`
				Num202310 string `json:"202310"`
				Num202311 string `json:"202311"`
				Num202312 string `json:"202312"`
				Num202401 string `json:"202401"`
				Num202402 string `json:"202402"`
				Num202403 string `json:"202403"`
				Num202404 string `json:"202404"`
				Num202405 string `json:"202405"`
				Num202406 string `json:"202406"`
				Num202407 string `json:"202407"`
				Num202408 string `json:"202408"`
				Num202409 string `json:"202409"`
				Num202410 string `json:"202410"`
				Num202411 string `json:"202411"`
				Num202412 string `json:"202412"`
				Num202501 string `json:"202501"`
				Num202502 string `json:"202502"`
				Num202503 string `json:"202503"`
				Num202504 string `json:"202504"`
				Num202505 string `json:"202505"`
				Num202506 string `json:"202506"`
				Num202507 string `json:"202507"`
				Num202508 string `json:"202508"`
				Num202509 string `json:"202509"`
				Num202510 string `json:"202510"`
				Num202511 string `json:"202511"`
				Num202512 string `json:"202512"`
				Num202601 string `json:"202601"`
				Num202602 string `json:"202602"`
				Num202603 string `json:"202603"`
				Num202604 string `json:"202604"`
				Num202605 string `json:"202605"`
				Num202606 string `json:"202606"`
				Num202607 string `json:"202607"`
				Num202608 string `json:"202608"`
				Num202609 string `json:"202609"`
				Num202610 string `json:"202610"`
				Num202611 string `json:"202611"`
				Num202612 string `json:"202612"`
			} `json:"serie"`
		} `json:"series"`
	} `json:"resultados"`
}

type DataINPC struct {
	Referencia        string  `json:"referencia" csv:"referencia"`
	Ano               string  `json:"ano" csv:"ano"`
	Mes               string  `json:"mes" csv:"mes"`
	Variacao          float64 `json:"variacao" csv:"variacao"`
	AcumuladoAno      float64 `json:"acumulado_ano" csv:"acumulado_ano"`
	Acumulado12Meses  float64 `json:"acumulado_doze_meses" csv:"acumulado_doze_meses"`
	ConsolidacaoAno   bool    `json:"consolidado_ano" csv:"consolidado_ano"`
	IdentificadorIBGE string  `json:"identificador_ibge" csv:"identificador_ibge"`
}

type INPC struct {
	Atualizacao   time.Time  `json:"data_atualizacao"`
	UnidadeMedida string     `json:"unidade_medida"`
	Fonte         string     `json:"fonte"`
	Data          []DataIPCA `json:"data"`
}

func RunnerINPC() {

	runnerName := "INPC - Histórico"
	url := "https://servicodados.ibge.gov.br/api/v3/agregados/1736/periodos/-9999/variaveis/44|68|2292?localidades=N1[all]"
	unidadeMedida := "%"
	fonte := "https://servicodados.ibge.gov.br"
	file_path := "./data/inflacao/inpc.json"
	fileNameOutputCSV := "./data/inflacao/inpc.csv"

	ordenado := make(map[string]DataIPCA)

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	ipca := IPCA{}
	now := time.Now()
	ipca.Atualizacao = now
	ipca.Fonte = fonte
	ipca.UnidadeMedida = unidadeMedida

	var response IPCAResponse
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

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	names := structs.Names(response[0].Resultados[0].Series[0].Serie)

	// Criando o Map de Referencias
	for _, n := range names {
		item := DataIPCA{}

		anomes := strings.Replace(n, "Num", "", -1)
		ano := anomes[0:4]
		mes := anomes[4:6]

		referencia := fmt.Sprintf("%v-%v", ano, mes)
		item.Referencia = referencia
		item.Mes = mes
		item.Ano = ano
		item.IdentificadorIBGE = n

		if mes == "12" {
			item.ConsolidacaoAno = true
		} else {
			item.ConsolidacaoAno = false
		}

		ordenado[n] = item
	}

	for _, r := range response {

		// Variação Mensal
		if strings.HasSuffix(r.Variavel, "Variação mensal") {
			names := structs.Names(r.Resultados[0].Series[0].Serie)

			for _, n := range names {
				r := reflect.ValueOf(r.Resultados[0].Series[0].Serie)
				f := reflect.Indirect(r).FieldByName(n)

				valueRaw := f.String()

				if valueRaw == "..." || valueRaw == "" {
					continue
				}

				valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

				if err != nil {
					l.Error().
						Str("Runner", runnerName).
						Str("Error", err.Error()).
						Str("Valor recuperado", valueRaw).
						Msg("Erro ao converter o valor para Float64")
					continue
				}

				item := ordenado[n]
				item.Variacao = valor

				ordenado[n] = item
			}
		}

		// Acumulado Ano
		if strings.HasSuffix(r.Variavel, "Variação acumulada no ano") {
			names := structs.Names(r.Resultados[0].Series[0].Serie)

			for _, n := range names {
				r := reflect.ValueOf(r.Resultados[0].Series[0].Serie)
				f := reflect.Indirect(r).FieldByName(n)

				valueRaw := f.String()

				if valueRaw == "..." || valueRaw == "" {
					continue
				}

				valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

				if err != nil {
					l.Error().
						Str("Runner", runnerName).
						Str("Error", err.Error()).
						Str("Valor recuperado", valueRaw).
						Msg("Erro ao converter o valor para Float64")
					continue
				}

				item := ordenado[n]
				item.AcumuladoAno = valor

				ordenado[n] = item
			}
		}

		// Acumulado 12 Meses
		if strings.HasSuffix(r.Variavel, "Variação acumulada em 12 meses") {
			names := structs.Names(r.Resultados[0].Series[0].Serie)

			for _, n := range names {
				r := reflect.ValueOf(r.Resultados[0].Series[0].Serie)
				f := reflect.Indirect(r).FieldByName(n)

				valueRaw := f.String()

				if valueRaw == "..." || valueRaw == "" {
					continue
				}

				valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

				if err != nil {
					l.Error().
						Str("Runner", runnerName).
						Str("Error", err.Error()).
						Str("Valor recuperado", valueRaw).
						Msg("Erro ao converter o valor para Float64")
					continue
				}

				item := ordenado[n]
				item.Acumulado12Meses = valor

				ordenado[n] = item
			}
		}

	}

	for _, i := range ordenado {
		if i.AcumuladoAno == 0 && i.Acumulado12Meses == 0 && i.Variacao == 0 {
			continue
		}

		ipca.Data = append(ipca.Data, i)
	}

	// Sort do data
	sort.Slice(ipca.Data, func(i, j int) bool {
		return ipca.Data[i].Referencia < ipca.Data[j].Referencia
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(ipca)
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

	csvOutput, err := gocsv.MarshalString(&ipca.Data)
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
		Msg("Finalizado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")
}
