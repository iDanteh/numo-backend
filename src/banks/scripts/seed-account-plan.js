'use strict';

/**
 * banks/scripts/seed-account-plan.js
 * ─────────────────────────────────────────────────────────────────────────────
 * Siembra el catálogo de cuentas contables y centros de costo en PostgreSQL.
 *
 * - Seguro de ejecutar múltiples veces (idempotente).
 * - Usa upsert con conflicto en 'codigo' / 'clave'.
 * - El campo parentId se resuelve automáticamente a partir de ctaMayor.
 *
 * Uso:
 *   node src/banks/scripts/seed-account-plan.js
 */

require('dotenv').config();

const { AccountPlan, CentroCosto } = require('../../shared/models/postgres');

// ── Catálogo de cuentas (1089 cuentas) ───────────────────────────────────────
const CUENTAS = [
  {
    "codigo": "1000000000",
    "nombre": "ACTIVO",
    "ctaMayor": null
  },
  {
    "codigo": "1100000000",
    "nombre": "CIRCULANTE",
    "ctaMayor": "1000000000"
  },
  {
    "codigo": "1101000000",
    "nombre": "CAJA y EQUIVALENTES",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1101010000",
    "nombre": "Caja y Equivalentes",
    "ctaMayor": "1101000000"
  },
  {
    "codigo": "1101010001",
    "nombre": "Caja",
    "ctaMayor": "1101010000"
  },
  {
    "codigo": "1101010002",
    "nombre": "Fondo Fijo",
    "ctaMayor": "1101010000"
  },
  {
    "codigo": "1101010003",
    "nombre": "Caja por identificar",
    "ctaMayor": "1101010000"
  },
  {
    "codigo": "1102000000",
    "nombre": "BANCOS E INVERSION",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1102010000",
    "nombre": "Bancos Moneda Nacional",
    "ctaMayor": "1102000000"
  },
  {
    "codigo": "1102010004",
    "nombre": "Bancos por identificar",
    "ctaMayor": "1102010000"
  },
  {
    "codigo": "1102011001",
    "nombre": "BBVA Bancomer Cta. 0109031014",
    "ctaMayor": "1102011000"
  },
  {
    "codigo": "1102011002",
    "nombre": "BBVA Bancomer Nomina Cta. 0110128112",
    "ctaMayor": "1102011000"
  },
  {
    "codigo": "1102011003",
    "nombre": "BBVA Bancomer Tarjeta Versatil Cta. 0109196455",
    "ctaMayor": "1102011000"
  },
  {
    "codigo": "1102011004",
    "nombre": "BBVA Bancomer Tarjeta Periferica Cta.1504555341",
    "ctaMayor": "1102011000"
  },
  {
    "codigo": "1102011005",
    "nombre": "Bancos por identificar",
    "ctaMayor": "1102000000"
  },
  {
    "codigo": "1102012001",
    "nombre": "Banamex Cta. 120-7746971",
    "ctaMayor": "1102012000"
  },
  {
    "codigo": "1102013001",
    "nombre": "Santander Serfin Cta. 65505865405",
    "ctaMayor": "1102013000"
  },
  {
    "codigo": "1102014001",
    "nombre": "Banorte Cta. 0318581703",
    "ctaMayor": "1102014000"
  },
  {
    "codigo": "1102015001",
    "nombre": "Scotiabank Inverlat Cta.",
    "ctaMayor": "1102015000"
  },
  {
    "codigo": "1102016001",
    "nombre": "Azteca Cta. 0153934698",
    "ctaMayor": "1102016000"
  },
  {
    "codigo": "1102020000",
    "nombre": "Cuentas de Inversion",
    "ctaMayor": "1102000000"
  },
  {
    "codigo": "1102021001",
    "nombre": "Banamex Inversion 161246978",
    "ctaMayor": "1102021000"
  },
  {
    "codigo": "1102021002",
    "nombre": "Banamex Inversion 9692381019",
    "ctaMayor": "1102021000"
  },
  {
    "codigo": "1102022001",
    "nombre": "Bancomer Inversion 2055893577",
    "ctaMayor": "1102022000"
  },
  {
    "codigo": "1102023001",
    "nombre": "Santander Inversion 66505865405",
    "ctaMayor": "1102023000"
  },
  {
    "codigo": "1103000000",
    "nombre": "CUENTAS POR COBRAR",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1103010000",
    "nombre": "Clientes Nacionales General",
    "ctaMayor": "1103000000"
  },
  {
    "codigo": "1103010001",
    "nombre": "Clientes Nacionales General Tasa 16 %",
    "ctaMayor": "1103010000"
  },
  {
    "codigo": "1103010002",
    "nombre": "Clientes Nacionales General Tasa 0 %",
    "ctaMayor": "1103010000"
  },
  {
    "codigo": "1103010003",
    "nombre": "Clientes Nacionales General Otros Servicios",
    "ctaMayor": "1103010000"
  },
  {
    "codigo": "1103020000",
    "nombre": "Clientes Intercompañias",
    "ctaMayor": "1103000000"
  },
  {
    "codigo": "1103020001",
    "nombre": "Clientes Intercompañias Tasa 16 %",
    "ctaMayor": "1103020000"
  },
  {
    "codigo": "1103020002",
    "nombre": "Clientes Intercompañias Tasa 0 %",
    "ctaMayor": "1103020000"
  },
  {
    "codigo": "1103020003",
    "nombre": "Clientes Intercompañias Otros Servicios",
    "ctaMayor": "1103020000"
  },
  {
    "codigo": "1103030000",
    "nombre": "Cobranza En Legal",
    "ctaMayor": "1103000000"
  },
  {
    "codigo": "1103030001",
    "nombre": "Cobranza En Legal",
    "ctaMayor": "1103030000"
  },
  {
    "codigo": "1103040000",
    "nombre": "Provision Incobrables",
    "ctaMayor": "1103000000"
  },
  {
    "codigo": "1103040001",
    "nombre": "Provision Incobrables",
    "ctaMayor": "1103040000"
  },
  {
    "codigo": "1103990000",
    "nombre": "Otras cuentas y documentos por cobrar",
    "ctaMayor": "1103000000"
  },
  {
    "codigo": "1103990001",
    "nombre": "Otras cuentas y documentos por cobrar",
    "ctaMayor": "1103990000"
  },
  {
    "codigo": "1104000000",
    "nombre": "DEUDORES DIVERSOS",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1104010000",
    "nombre": "Funcionarios Y Empleados",
    "ctaMayor": "1104000000"
  },
  {
    "codigo": "1104010001",
    "nombre": "Funcionarios Y Empleados General",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010002",
    "nombre": "Aguilar Hernandez Venancia",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010003",
    "nombre": "Avendaño Medina Marco Aurelio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010004",
    "nombre": "Benitez Baena Luis Alejandro",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010005",
    "nombre": "Caballero Hernandez Jose Antonio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010006",
    "nombre": "Cabrera Hernandez Gil",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010007",
    "nombre": "Cabrera Martinez Gil",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010008",
    "nombre": "Camacho Christy Jose Roberto",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010009",
    "nombre": "Casarin Rodriguez Uziel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010010",
    "nombre": "Castro Perez Luis Gabriel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010011",
    "nombre": "Cerero Ortega Oswaldo",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010012",
    "nombre": "Cervantes Cruz Jorge",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010013",
    "nombre": "Chavez Santos Enrique Daniel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010014",
    "nombre": "Cruz Agudo Armando",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010015",
    "nombre": "Cruz Garcia Caleb",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010016",
    "nombre": "Cruz Illescas Aurora Karina",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010017",
    "nombre": "Cruz Zarate Josue",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010018",
    "nombre": "Cuevas Martinez Argelia",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010019",
    "nombre": "Davila Martinez Karla Jeannet",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010020",
    "nombre": "Davila Martinez Luis Alberto",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010021",
    "nombre": "España Vasquez Fredy Eric",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010022",
    "nombre": "Febronio Esteva Edgar",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010023",
    "nombre": "Figueroa Crespo Gerardo Giovanni",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010024",
    "nombre": "Flores Martinez Rosalia",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010025",
    "nombre": "Fuentes Cordero Yobani Rigoberto",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010026",
    "nombre": "Gallegos Gonzalez Angel Adan",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010027",
    "nombre": "Garcia Mendoza Juan Antonio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010028",
    "nombre": "Garcia Santiago Juan Cristobal",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010029",
    "nombre": "Garcia Toral Elmi",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010030",
    "nombre": "Godinez Santome ignacio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010031",
    "nombre": "Gonzalez Ramirez Ivan",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010032",
    "nombre": "Hernandez Ceballos Luis Angel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010033",
    "nombre": "Hernandez Hernandez Martina",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010034",
    "nombre": "Herrera Marquez Julio Javier",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010035",
    "nombre": "Jimenez Ramirez Liliana",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010036",
    "nombre": "Juarez Romero Haroldo",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010037",
    "nombre": "Laynez Vera Emilio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010038",
    "nombre": "Leyva Lopez Juan Manuel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010039",
    "nombre": "Lopez Hernandez Juan",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010040",
    "nombre": "Lopez Lopez Francisco",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010041",
    "nombre": "Lopez Molina Victoria",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010042",
    "nombre": "Lopez Reyes Yair Cristian",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010043",
    "nombre": "Lopez Sarmiento Ramon",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010044",
    "nombre": "Martinez Martinez Alfredo",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010045",
    "nombre": "Martinez Torres Pascual",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010046",
    "nombre": "Matias Antonio Francisco Jose",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010047",
    "nombre": "Mendez Perez Pedro Fernando",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010048",
    "nombre": "Merino Hernandez Nemesio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010049",
    "nombre": "Mesinas Martinez Francisca",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010050",
    "nombre": "Miguel Sanchez Evelio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010051",
    "nombre": "Mora Castañeda Ivan",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010052",
    "nombre": "Morales Gutierrez Roman",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010053",
    "nombre": "Morales Gutierrez Seferino",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010054",
    "nombre": "Nuñez Garcia Ivan Israel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010055",
    "nombre": "Ortiz Molina Gersain",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010056",
    "nombre": "Pablo Sanchez Itzel Monserrat",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010057",
    "nombre": "Perez Edgar Elizabeth Rosalba",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010058",
    "nombre": "Perez Hernandez Marcos Giovani",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010059",
    "nombre": "Perez Ruiz Erik De Jesus",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010060",
    "nombre": "Perez Velasco Efren",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010061",
    "nombre": "Ramirez Chavez Paola Nalleli",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010062",
    "nombre": "Ramirez Villanueva Ignacio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010063",
    "nombre": "Ramos Guerrero Leonides Armando",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010064",
    "nombre": "Reyes Cancio Alma Alejandra",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010065",
    "nombre": "Reyes Lopez Alejandra Guadalupe",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010066",
    "nombre": "Reyes Ruiz Iris Rubicela",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010067",
    "nombre": "Rios Fernandez Humberto",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010068",
    "nombre": "Robles Pacheco Jose",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010069",
    "nombre": "Rojas Carrasco Luz Elena",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010070",
    "nombre": "Rojas Ziga Octavio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010071",
    "nombre": "Roque Velazquez Alberto Isaias",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010072",
    "nombre": "Rosette Cabrera Aracely",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010073",
    "nombre": "Salinas Damian Adelfa",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010074",
    "nombre": "Sanchez Hernandez Manglio Fernando",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010075",
    "nombre": "Sanchez Illescas Eliel Antonio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010076",
    "nombre": "Sanchez Solano Lidia Vianey",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010077",
    "nombre": "Sanchez Vasquez Saul",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010078",
    "nombre": "Sebastian Garcia Jose Moises",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010079",
    "nombre": "Sierra Rios Pedro",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010080",
    "nombre": "Torrez Lopez Juan Antonio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010081",
    "nombre": "Ulloa Palacios Maria De Jesus",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010082",
    "nombre": "Valle Dominguez Arturo",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010083",
    "nombre": "Vargas Zarate Wilber",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010084",
    "nombre": "Vasquez Cernas Dorian Emmanuel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010085",
    "nombre": "Vasquez Diaz Ignacio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010086",
    "nombre": "Vasquez Lopez Fernando",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010087",
    "nombre": "Villalobos Jimenez Jose Maria",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010088",
    "nombre": "Zarate Enriquez Alfonso Antonio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010089",
    "nombre": "Zuñiga Toledo Oscar Esteban",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010090",
    "nombre": "Perez Perez Carlos",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010091",
    "nombre": "Villegas Fuentes Irbin Rolando",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010092",
    "nombre": "Lopez Lopez Gustavo Abel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010093",
    "nombre": "Rosales Ramirez Walter Gerardo",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010094",
    "nombre": "Espinoza Rojas Rosaura del Carmen",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010095",
    "nombre": "Sanchez Lopez Jose Angel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010096",
    "nombre": "Morales Castellanos Cristian Daniel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010097",
    "nombre": "Bautista Domingo Pedro",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010098",
    "nombre": "Ramirez Jose Gabriel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010099",
    "nombre": "Cruz Cruz Adalberto",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010100",
    "nombre": "Cruz Velasco David Guillen",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010101",
    "nombre": "Toledo Antonio Hiran",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010102",
    "nombre": "Gomez Santis Juana",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010103",
    "nombre": "Jimenez Garcia Irving",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010104",
    "nombre": "Reyes Ruiz Andres Pedro",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010105",
    "nombre": "Caballero Cruz Raul",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010106",
    "nombre": "Garcia Garcia Feliciano",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010107",
    "nombre": "Chavez Ortega Queisy Raquel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010108",
    "nombre": "Rios Medina Jesus Jovani",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010109",
    "nombre": "Anaya Ramirez Osvaldo",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010110",
    "nombre": "Quiroz Soto Miguel Angel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010111",
    "nombre": "Bonequi Trujillo Andres Enrique",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010112",
    "nombre": "Roman Burciaga Pablo Alberto",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010113",
    "nombre": "Gallardo Zaragoza Stefania",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010114",
    "nombre": "Sosa Ramirez Marcos",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010115",
    "nombre": "Caballero Monjaraz Antonio",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010116",
    "nombre": "Rubio Palma Karen Stephany",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010117",
    "nombre": "Hernandez Manzano Ivon",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010118",
    "nombre": "Quiroz Vasquez Shany Danee",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010119",
    "nombre": "Lopez Trujillo Noemi",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010120",
    "nombre": "San Juan Perez Jonathan Emmanuel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010121",
    "nombre": "Contreras Pensamiento Raymundo",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010122",
    "nombre": "Martinez Chavez Juan Pablo",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010123",
    "nombre": "Carrasco Cruz Hebert",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010124",
    "nombre": "Merchan Morales Erick Eduardo",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010125",
    "nombre": "Gayosso Lovera Ana",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010126",
    "nombre": "Orocio Sanchez Catalina",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010127",
    "nombre": "Arellanes Gomez Diana",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010128",
    "nombre": "Cantera Ramirez Dalila Teresita",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010129",
    "nombre": "Lopez Cruz Rosalba",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010130",
    "nombre": "Montes Cano Joaquin",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010131",
    "nombre": "Jimenez Ignacio Gabriel Adan",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010132",
    "nombre": "Lopez Carrasco Everardo",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010133",
    "nombre": "Santiago Rojas Francisco Javier",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010134",
    "nombre": "Cruz Garcia Miguel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010135",
    "nombre": "Miguel Rios Juan Carlos",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010136",
    "nombre": "Diaz Flores Diego",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010137",
    "nombre": "Contreras Landes Evelio",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010138",
    "nombre": "Gutierrez Garcia Aurelio",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010139",
    "nombre": "Ortiz Molina Miguel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010140",
    "nombre": "Santos Quevedo Angel Felix",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010141",
    "nombre": "Lopez Luis Jorge",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010142",
    "nombre": "Palacios Garnica Eric Miguel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010143",
    "nombre": "Perez Rosales Ulises Javier",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010144",
    "nombre": "Lopez Villatoro Sergio",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010145",
    "nombre": "Almaraz Luna Isaias Sadot",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010146",
    "nombre": "Ramirez Santos Dora Maria",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010147",
    "nombre": "Bernal Perez Soledad",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010148",
    "nombre": "Bautista Chavez Benito",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010149",
    "nombre": "Gaitan Juana",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010150",
    "nombre": "Martinez Carmona Eliud Alejandro",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010151",
    "nombre": "Caballero Monjaraz Ruben",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010152",
    "nombre": "Martinez Mendez Jorge",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010153",
    "nombre": "Chavez Chavez Gregorio Valentin",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010154",
    "nombre": "Hernandez Pacheco Juan",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010155",
    "nombre": "Sanchez Santiago Narciso Rafael",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010156",
    "nombre": "Sandoval Barrios Salvador",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010157",
    "nombre": "Rosas Omar",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010158",
    "nombre": "Valdez Garcia Jose Daniel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010159",
    "nombre": "Mendez Diaz Oscar Humberto",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010160",
    "nombre": "Mireles Jaramillo Griselda Liviet",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010161",
    "nombre": "Perez Mendoza Oscar Uriel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010162",
    "nombre": "Gallegos Altamirano Ivan",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010163",
    "nombre": "Ramirez Pacheco Javier",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010164",
    "nombre": "Chavez Terriquez Nancy Guadalupe",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010165",
    "nombre": "Carrasco Coronado Isaias Benjamin",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010166",
    "nombre": "Garcia Toral Robertony",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010167",
    "nombre": "Esperon Hernandez Jose Antonio",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010168",
    "nombre": "Perez Zarate Floridelfa",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010169",
    "nombre": "Blanco Ruiz Melina",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010170",
    "nombre": "Avila Lozano Marvin Barush",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010171",
    "nombre": "Garcia Hernandez Uriel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010172",
    "nombre": "Jove Torres Karen",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010173",
    "nombre": "Lopez Villatoro Rafael",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010174",
    "nombre": "Fausto Sanchez Antonio",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010175",
    "nombre": "Santiago Martinez Sergio Antonio",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010176",
    "nombre": "Morales Ramirez Erik de Jesus",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010177",
    "nombre": "Clavel Santiago Maria Isabel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010178",
    "nombre": "Hernandez Velazco Salomon",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010179",
    "nombre": "Hernandez Luna Mitzi Yareni",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010180",
    "nombre": "Antonio Velasco Edgar",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010181",
    "nombre": "Perez Poblete Alberto Angel",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010182",
    "nombre": "Vasquez Ramirez Kevin Adonais",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010183",
    "nombre": "Enriquez Salinas David",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010184",
    "nombre": "Gomez Mercado Isaias",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010185",
    "nombre": "Garcia Aguilar Carmen Coilee",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010186",
    "nombre": "Lucero Diaz Jose Enrique",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010187",
    "nombre": "De Leon Gomez Marlon Arturo",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010188",
    "nombre": "Esteva Acevedo Jorge Ivan",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010189",
    "nombre": "Martinez Ramirez Jose Antonio",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010190",
    "nombre": "Hernandez Aquino Julio Cesar",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010191",
    "nombre": "Luis Perez Felipe Floriberto",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010192",
    "nombre": "Zarate Ortiz Julio Cesar",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010193",
    "nombre": "Guzman Castillo Carlos",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010194",
    "nombre": "Peña Mendoza Luis",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010195",
    "nombre": "Prudencio Cecilio Jose Aldair",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010196",
    "nombre": "Moran Salinas Eduardo",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010197",
    "nombre": "Lopez Cruz Eugenio David",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010198",
    "nombre": "Hernandez Lopez Patricia",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010199",
    "nombre": "Bautista Becerra Isidro",
    "ctaMayor": "1104010100"
  },
  {
    "codigo": "1104010200",
    "nombre": "Ricardo Antonio Luria Garcia",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010201",
    "nombre": "Aguilar Cruz Tania Izbeth",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010202",
    "nombre": "Aguilar Trujillo Socrates de Jesus",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010203",
    "nombre": "Santiago Martinez Lourdes Cecilia",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010204",
    "nombre": "Cuevas Bedford Miriam del Carmen",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010205",
    "nombre": "Aguilar Garcia Omar",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010206",
    "nombre": "Amador Mendez Julio Cesar",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010207",
    "nombre": "Guerrero Ruiz Julio Cesar",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010208",
    "nombre": "Reyes Pacheco Gamaliel Santiago",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010209",
    "nombre": "Santiago Santiago Maricruz",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010210",
    "nombre": "Lopez Esteva Jose Javier",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010211",
    "nombre": "Matuz Villegas Luvia",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010212",
    "nombre": "Garcia Luria Ricardo Antonio",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010213",
    "nombre": "Martinez Garcia Antonio",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010214",
    "nombre": "Paz Lopez Brenda Itzel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010215",
    "nombre": "Gutierrez Sierra Irving",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010216",
    "nombre": "Ramirez Magaña Cristian Eduardo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010217",
    "nombre": "Sanchez Moscoso Yurani Antonia",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010218",
    "nombre": "Santiago Bautista Julio Cesar",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010219",
    "nombre": "Martinez Martinez Norberto",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010220",
    "nombre": "Velazquez Huerta Raul",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010221",
    "nombre": "Zarate Antonio Marisol",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010222",
    "nombre": "Sanchez Cedillo Luis Alberto",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010223",
    "nombre": "Santiago Cruz Miguel Angel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010224",
    "nombre": "Ramirez Pacheco Andres",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010225",
    "nombre": "Bautista Garcia Manuel Arturo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010226",
    "nombre": "Cruz Garcia Lorenzo Alejandro",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010227",
    "nombre": "Luna Perez Karen Leylan",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010228",
    "nombre": "Sanchez Ojeda Mariana",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010229",
    "nombre": "Ramirez Ruiz Arturo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010230",
    "nombre": "Perez Poblete Sergio Francisco",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010231",
    "nombre": "Madrid Lopez Julio Cesar",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010232",
    "nombre": "Gomez Duran Mercedes Paula",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010233",
    "nombre": "Reyes Ruiz Claudia",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010234",
    "nombre": "Ramos Martinez Samuel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010235",
    "nombre": "Lopez Zarate Flor de Liz",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010236",
    "nombre": "Mendez Garcia Jair de Jesus",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010237",
    "nombre": "Maldonado Ramirez Hugo Alexander",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010238",
    "nombre": "Gabriel Ramos Angel Daniel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010239",
    "nombre": "Gonzalez Zaragoza German",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010240",
    "nombre": "Hernandez Jacinto Jorge Uriel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010241",
    "nombre": "Gabriel Garcia Faustino",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010242",
    "nombre": "Julian Guzman Daniel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010243",
    "nombre": "Ramirez Rodriguez Raymundo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010244",
    "nombre": "Gomez Jimenez Nancy",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010245",
    "nombre": "Cruz Alfaro Anthony",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010246",
    "nombre": "Perez Cruz Julieta",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010247",
    "nombre": "Reyes Luis Claudia",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010248",
    "nombre": "Serrano Chavez Fernando Alfredo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010249",
    "nombre": "Cruz Mendez Aurea",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010250",
    "nombre": "Figueroa Cruz Heidy Yosely",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010251",
    "nombre": "Monjaraz Lopez Joel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010252",
    "nombre": "Cortez Lopez Fernando",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010253",
    "nombre": "Rodriguez Arroyo Cristian Ivan",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010254",
    "nombre": "Irineo Mendez Jonathan Israel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010255",
    "nombre": "Guerrero Reyes Juan Adrian",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010256",
    "nombre": "Zarate Gomez Juan Carlos",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010257",
    "nombre": "Carrasco Lopez Carlos Arturo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010258",
    "nombre": "Gutierrez Martinez Lurencio",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010259",
    "nombre": "Vasquez Perez Gilberto Hugo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010260",
    "nombre": "Carballo Ramirez Antonio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010261",
    "nombre": "Mendez Hernandez Jesus Jaime",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010262",
    "nombre": "Flores Hernandez Amado",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010263",
    "nombre": "Gaytan Lopez Juan Carlos",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010264",
    "nombre": "Vidal Nakamura Nestor Justino",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010265",
    "nombre": "Cruz Paz Felipe",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010266",
    "nombre": "Carrillo Reyes Javier Alberto",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010267",
    "nombre": "Vicente Islas Luis Alberto",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010268",
    "nombre": "Chavez Ulloa Victor Hugo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010269",
    "nombre": "Ruiz Jimenez Daniel Jesus",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010270",
    "nombre": "Martinez Garcia Faustino",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010271",
    "nombre": "Santiago Perez Victor Hugo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010272",
    "nombre": "Tenorio Dominguez Hector Daniel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010273",
    "nombre": "Ruiz Ramirez Magdalena",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010274",
    "nombre": "Garcia Garcia Alexis Omar",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010275",
    "nombre": "Villanueva Robles Mayrani Arizbeth",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010276",
    "nombre": "Cortinez Hernandez Jose Luis",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010277",
    "nombre": "Miguel Candela Daniel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010278",
    "nombre": "Costumbre Morgado Enrique",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010279",
    "nombre": "Castro Lopez Ivan",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010280",
    "nombre": "Bernabe Matias Luis Fernando",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010281",
    "nombre": "Castro Perez Alberto",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010282",
    "nombre": "Martinez Gonzalez Abelardo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010283",
    "nombre": "Azcona Marco Antonio",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010284",
    "nombre": "Lopez Ramirez Luis Angel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010285",
    "nombre": "Lopez Geminiano Josue elias",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010286",
    "nombre": "Martinez Flores Eliseo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010287",
    "nombre": "Santiago Santiago Ricardo Augusto",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010288",
    "nombre": "Martinez Hernandez Jair Uriel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010289",
    "nombre": "Espinoza Ogarrio Roberto Izteotl",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010290",
    "nombre": "Lopez Jimenez David Eliel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010291",
    "nombre": "Lopez Jimenez Joel Yogtan",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010292",
    "nombre": "Hernandez Garcia Julio Cesar",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010293",
    "nombre": "Rasgado Martínez Joseph",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010294",
    "nombre": "Garcia Cruz Jesus Oswaldo",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010295",
    "nombre": "Mendez Mateo Mario Emmanuel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010296",
    "nombre": "Luna Herendia Kenia Irais",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010297",
    "nombre": "Ramirez Cruz Pedro Israel",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010298",
    "nombre": "Salazar Garcia Guillermo Guadalupe",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010299",
    "nombre": "Santiago Nicolas Cesar",
    "ctaMayor": "1104010200"
  },
  {
    "codigo": "1104010300",
    "nombre": "Mateo Dionicio Rey",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010301",
    "nombre": "Jimenez Cruz Aurea Soledad",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010302",
    "nombre": "Hernandez Garcia Juan",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010303",
    "nombre": "Morales Paz Jonathan",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010304",
    "nombre": "Ortiz Ortega Jorge Emilio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010305",
    "nombre": "Bautista Sosa Ramon",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010306",
    "nombre": "Aparicio Bautista Jose Luis",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010307",
    "nombre": "Pascual Jimenez Daniel Ademar",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010308",
    "nombre": "Alcantara Garcia Gil Alejandro",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010309",
    "nombre": "Perez Mendoza Carlos Daniel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010310",
    "nombre": "Pacheco Jimenez Juan de Dios",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010311",
    "nombre": "Lopez Velasco Diego Jesus",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010312",
    "nombre": "Vazquez Juanes Armando",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010313",
    "nombre": "Vasquez Mendez Ulises Ismael",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010314",
    "nombre": "Quintas Ruiz Hector Gerardo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010315",
    "nombre": "Gamboa Cazarin Omar",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010316",
    "nombre": "Morales Gaspar Raul",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010317",
    "nombre": "Hernandez Diego Rey Fernando",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010318",
    "nombre": "Ortiz Blanco Jesus Miguel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010319",
    "nombre": "Hernandez Morales Leonardo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010320",
    "nombre": "Alducin Ortega Carlos Rodrigo",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010321",
    "nombre": "Escalante Perez Eduardo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010322",
    "nombre": "Juarez Cruz Ramiro Daniel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010323",
    "nombre": "Miranda Carrasco Jorge Arturo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010324",
    "nombre": "Hernandez Arguelles Tomas Fernando",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010325",
    "nombre": "Lagunas Audifred Ricardo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010326",
    "nombre": "Arciga Reyes Alejandra",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010327",
    "nombre": "Altamirano Luis Juan Antonio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010328",
    "nombre": "Avendaño Quintero Ernesto Daniel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010329",
    "nombre": "Palma Figueroa Alejandro",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010330",
    "nombre": "Jimenez Avelar Erick Noel",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010331",
    "nombre": "Aquino Chinas Vicente",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010332",
    "nombre": "Miguel Hernandez Miguel Angel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010333",
    "nombre": "Cruz Miguel Angel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010334",
    "nombre": "Olazo Jimenez Luis Rafael",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010335",
    "nombre": "Hernandez Hernandez Luis Antonio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010336",
    "nombre": "Sandoval Olvera Ignacio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010337",
    "nombre": "Rivera Diaz Jose Otilio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010338",
    "nombre": "Figueroa Pacheco Rodrigo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010339",
    "nombre": "Ruiz Medina Oscar Hugo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010340",
    "nombre": "Jimenez Avendaño Alejandro de Jesus",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010341",
    "nombre": "Ruiz Hernandez Victor Alonso",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010342",
    "nombre": "Rivas Almaza Jonatan Martin",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010343",
    "nombre": "Garcia Garcia Luis Angel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010344",
    "nombre": "Garcia Jimenez Victor Alfonso",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010345",
    "nombre": "Marin Martinez Daniel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010346",
    "nombre": "Lopez Vergara Brisa Vanessa",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010347",
    "nombre": "Caballero Diaz Jose de Jesus",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010348",
    "nombre": "Perez Bautista Fili Uriel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010349",
    "nombre": "Martinez Zaguilan Carlos Elias",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010350",
    "nombre": "Rutilo Coronel Cristhian",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010351",
    "nombre": "Martinez Hernandez Abigail Monserrat",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010352",
    "nombre": "Posada Monterrubio Antonio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010353",
    "nombre": "Cruz Figueroa Karina Gabriela",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010354",
    "nombre": "Velazquez Martinez Leodegario",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010355",
    "nombre": "Aragon Martinez Wilberth",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010356",
    "nombre": "Cervantes Hernandez Kevin Emmanuel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010357",
    "nombre": "Olmedo Rojas Olegario Yeysy",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010358",
    "nombre": "Garcia Palma Gonzalo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010359",
    "nombre": "Ambrosio Garcia Diego",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010360",
    "nombre": "Rosas Leon Jesus Alberto",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010361",
    "nombre": "Lopez Montero Dagoberto",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010362",
    "nombre": "Cruz Chagoya Marco Antonio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010363",
    "nombre": "Eugenio Rivera Fredy Uriel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010364",
    "nombre": "Perez Zarate Atdermi",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010365",
    "nombre": "Perez Mateo Luis Eduardo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010366",
    "nombre": "Santiago Perez Gamaliel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010367",
    "nombre": "Jimenez Almaraz Felix",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010368",
    "nombre": "Morales Guzman Juan Alberto",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010369",
    "nombre": "Hernandez Valencia Carlos Eduardo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010370",
    "nombre": "Torres Garcia Carlos Antonio",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010371",
    "nombre": "Martinez Arevalo Francisco Javier",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010372",
    "nombre": "Martinez Miguel Pedro",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010373",
    "nombre": "Mateo Dionicio Karina",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010374",
    "nombre": "Villanueva Cartas Roque",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010375",
    "nombre": "Montaño Martinez Jose Antonio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010376",
    "nombre": "Pinelo Pinelo Salvador David",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010377",
    "nombre": "Villegas Parada Eduardo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010378",
    "nombre": "Jimenez Lopez Emilton David",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010379",
    "nombre": "Romero Garcia Diego Erick",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010380",
    "nombre": "Reyes Martinez Alexis",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010381",
    "nombre": "Martinez Moreno Jezael",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010382",
    "nombre": "Lopez Meneses Jose Manuel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010383",
    "nombre": "Cruz Ortiz Luis Arturo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010384",
    "nombre": "Diaz Fernandez Reynaldo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010385",
    "nombre": "Virginia Zuñiga Wanda",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010386",
    "nombre": "Estrada Caballero Juan Carlos",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010387",
    "nombre": "Mata Salazar Argenis Jose",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010388",
    "nombre": "Romero Ruiz Rocio",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010389",
    "nombre": "Cruz Lopez Fernando Arturo",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010390",
    "nombre": "Mesinas Celaya Victor",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010391",
    "nombre": "Lopez Lopez Raul",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010392",
    "nombre": "Ruiz Merlin Raymundo Gabriel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010393",
    "nombre": "Sanchez Morales Luis Enrique",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010394",
    "nombre": "Jimenez Sosa Bruno Manuel",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010395",
    "nombre": "Villarreal Bustamante Bianca Olivia",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010396",
    "nombre": "Lopez Reyes Vicente",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010397",
    "nombre": "Lopez Hernandez Andrea Elizabeth",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010398",
    "nombre": "Enriquez Pedro Anahi Marlene",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010399",
    "nombre": "Zarate Rios Jorge Luis",
    "ctaMayor": "1104010300"
  },
  {
    "codigo": "1104010400",
    "nombre": "Jarquin Primo Pedro",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010401",
    "nombre": "Antonio Vicente Maria Elena",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010402",
    "nombre": "Cruz Cruz Areli",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010403",
    "nombre": "Garcia Gomez Vladimir",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010404",
    "nombre": "Ortega Juarez Jesus Adrian",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010405",
    "nombre": "Curiel Luna Edna Gabriela",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010406",
    "nombre": "Hernandez Espinoza Farith",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010407",
    "nombre": "Martinez Betanzos Aldis",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010408",
    "nombre": "Garcia Ramirez Efrain",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010409",
    "nombre": "Martinez Vasquez Carlos Alberto",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010410",
    "nombre": "Vargas Jimenez Javier Ulises",
    "ctaMayor": "1104010000"
  },
  {
    "codigo": "1104010411",
    "nombre": "Manzano Yescas Jasen Ulises",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010412",
    "nombre": "Dita Perez Francisco Javier",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010413",
    "nombre": "Ruiz Cruz Antonio Cesar",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010414",
    "nombre": "Trinidad Diaz Marzo Antonio",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104010415",
    "nombre": "Peralta Martinez Rosalia",
    "ctaMayor": "1104010400"
  },
  {
    "codigo": "1104020000",
    "nombre": "Deudores Diversos Intercompañias",
    "ctaMayor": "1104000000"
  },
  {
    "codigo": "1104020001",
    "nombre": "Deudores Diversos General",
    "ctaMayor": "1104020000"
  },
  {
    "codigo": "1104020002",
    "nombre": "Deudores Diversos Otros",
    "ctaMayor": "1104020000"
  },
  {
    "codigo": "1104030000",
    "nombre": "Deudores Diversos Terceros",
    "ctaMayor": "1104000000"
  },
  {
    "codigo": "1104031001",
    "nombre": "Tarjeta American Express",
    "ctaMayor": "1104031000"
  },
  {
    "codigo": "1104031002",
    "nombre": "Tarjeta Construganas",
    "ctaMayor": "1104031000"
  },
  {
    "codigo": "1104032001",
    "nombre": "Open Pay - Venta Enlinea",
    "ctaMayor": "1104032000"
  },
  {
    "codigo": "1104033001",
    "nombre": "MiTechito Capital SA de CV",
    "ctaMayor": "1104033000"
  },
  {
    "codigo": "1104040000",
    "nombre": "Gastos Por Comprobar",
    "ctaMayor": "1104000000"
  },
  {
    "codigo": "1104040001",
    "nombre": "Gastos Por Comprobar",
    "ctaMayor": "1104040000"
  },
  {
    "codigo": "1105000000",
    "nombre": "INVENTARIOS MERCANCiA",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1105010000",
    "nombre": "Almacen Principal Tasa 16%",
    "ctaMayor": "1105000000"
  },
  {
    "codigo": "1105010001",
    "nombre": "Almacen Principal Tasa 16%",
    "ctaMayor": "1105010000"
  },
  {
    "codigo": "1105020000",
    "nombre": "Almacen Principal Tasa 0%",
    "ctaMayor": "1105000000"
  },
  {
    "codigo": "1105020001",
    "nombre": "Almacen Principal Tasa 0%",
    "ctaMayor": "1105020000"
  },
  {
    "codigo": "1105030000",
    "nombre": "Mercancias en Transito Tasa 16 %",
    "ctaMayor": "1105000000"
  },
  {
    "codigo": "1105030001",
    "nombre": "Mercancias en Transito Tasa 16 %",
    "ctaMayor": "1105030000"
  },
  {
    "codigo": "1106000000",
    "nombre": "ANTICIPOS A PROVEEDORES",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1106010000",
    "nombre": "Anticipos A Proveedores",
    "ctaMayor": "1106000000"
  },
  {
    "codigo": "1106010001",
    "nombre": "Anticipos A Proveedores General",
    "ctaMayor": "1106010000"
  },
  {
    "codigo": "1106010002",
    "nombre": "Anticipos A Acreedor General",
    "ctaMayor": "1106010000"
  },
  {
    "codigo": "1106010003",
    "nombre": "Toka Internacional SAPI de CV",
    "ctaMayor": "1106010000"
  },
  {
    "codigo": "1106020000",
    "nombre": "Anticipos A Proveedores Intercompañia",
    "ctaMayor": "1106000000"
  },
  {
    "codigo": "1106020001",
    "nombre": "Anticipos A Proveedores Intercompañia",
    "ctaMayor": "1106020000"
  },
  {
    "codigo": "1107000000",
    "nombre": "IVA ACREDITABLE",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1107010000",
    "nombre": "IVA Acreditable",
    "ctaMayor": "1107000000"
  },
  {
    "codigo": "1107010001",
    "nombre": "IVA Acreditable",
    "ctaMayor": "1107010000"
  },
  {
    "codigo": "1108000000",
    "nombre": "IVA POR ACREDITAR",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1108010000",
    "nombre": "IVA Por Acreditar",
    "ctaMayor": "1108000000"
  },
  {
    "codigo": "1108010001",
    "nombre": "IVA Por Acreditar",
    "ctaMayor": "1108010000"
  },
  {
    "codigo": "1109000000",
    "nombre": "SALDOS A FAVOR DE IMPUESTOS",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1109010000",
    "nombre": "Saldos A Favor - ISR",
    "ctaMayor": "1109000000"
  },
  {
    "codigo": "1109010001",
    "nombre": "Saldos A Favor - ISR - 2009",
    "ctaMayor": "1109010000"
  },
  {
    "codigo": "1109010002",
    "nombre": "Saldos A Favor - ISR - 2011",
    "ctaMayor": "1109010000"
  },
  {
    "codigo": "1109010003",
    "nombre": "Saldos A Favor - ISR - 2012",
    "ctaMayor": "1109010000"
  },
  {
    "codigo": "1109010004",
    "nombre": "Saldos A Favor - ISR - 2018",
    "ctaMayor": "1109010000"
  },
  {
    "codigo": "1109010005",
    "nombre": "Saldos A Favor - ISR Retenciones",
    "ctaMayor": "1109010000"
  },
  {
    "codigo": "1109010006",
    "nombre": "Saldos A Favor - ISR - 2021",
    "ctaMayor": "1109010000"
  },
  {
    "codigo": "1109010007",
    "nombre": "Saldos A Favor - ISR - 2022",
    "ctaMayor": "1109010000"
  },
  {
    "codigo": "1109010008",
    "nombre": "Saldos A Favor - ISR 2023",
    "ctaMayor": "1109010000"
  },
  {
    "codigo": "1109020000",
    "nombre": "Saldos A Favor - IVA",
    "ctaMayor": "1109000000"
  },
  {
    "codigo": "1109020001",
    "nombre": "Saldos A Favor - IVA - 2012",
    "ctaMayor": "1109020000"
  },
  {
    "codigo": "1109020002",
    "nombre": "Saldos A Favor - IVA - 2013",
    "ctaMayor": "1109020000"
  },
  {
    "codigo": "1109020003",
    "nombre": "Saldos A Favor - IVA - 2014",
    "ctaMayor": "1109020000"
  },
  {
    "codigo": "1109020004",
    "nombre": "Saldos A Favor - IVA - 2015",
    "ctaMayor": "1109020000"
  },
  {
    "codigo": "1109020005",
    "nombre": "Saldos A Favor - IVA - 2016",
    "ctaMayor": "1109020000"
  },
  {
    "codigo": "1109020006",
    "nombre": "Saldos A Favor - IVA - 2017",
    "ctaMayor": "1109020000"
  },
  {
    "codigo": "1109020007",
    "nombre": "Saldos A Favor - IVA - 2021",
    "ctaMayor": "1109020000"
  },
  {
    "codigo": "1109030000",
    "nombre": "Saldos A Favor - IETU",
    "ctaMayor": "1109000000"
  },
  {
    "codigo": "1109030001",
    "nombre": "Saldos A Favor - IETU - 2009",
    "ctaMayor": "1109030000"
  },
  {
    "codigo": "1109040000",
    "nombre": "Pago de lo Indebido",
    "ctaMayor": "1109000000"
  },
  {
    "codigo": "1109040001",
    "nombre": "Pago de lo Indebido ISR",
    "ctaMayor": "1109040000"
  },
  {
    "codigo": "1109040002",
    "nombre": "Pago de lo Indebido IVA",
    "ctaMayor": "1109040000"
  },
  {
    "codigo": "1110000000",
    "nombre": "IMPUESTOS POR ANTICIPADO",
    "ctaMayor": "1000000000"
  },
  {
    "codigo": "1110010000",
    "nombre": "Provisionales de ISR",
    "ctaMayor": "1110000000"
  },
  {
    "codigo": "1110010001",
    "nombre": "Provisionales de ISR - Por Anticipado",
    "ctaMayor": "1110010000"
  },
  {
    "codigo": "1110010002",
    "nombre": "Provisionales de ISR - Bancarios",
    "ctaMayor": "1110010000"
  },
  {
    "codigo": "1111000000",
    "nombre": "SUBSIDIO PARA EL EMPLEO",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1111010000",
    "nombre": "Subsidio Para El Empleo",
    "ctaMayor": "1111000000"
  },
  {
    "codigo": "1111010001",
    "nombre": "Subsidio Para El Empleo",
    "ctaMayor": "1111010000"
  },
  {
    "codigo": "1112000000",
    "nombre": "INVERSION EN ACCIONES",
    "ctaMayor": "1100000000"
  },
  {
    "codigo": "1112010000",
    "nombre": "Inversion En Acciones",
    "ctaMayor": "1112000000"
  },
  {
    "codigo": "1112010001",
    "nombre": "Inversion En Acciones",
    "ctaMayor": "1112010000"
  },
  {
    "codigo": "1200000000",
    "nombre": "FIJO",
    "ctaMayor": "1000000000"
  },
  {
    "codigo": "1201000000",
    "nombre": "PROPIEDADES, PLANTA Y EQUIPO",
    "ctaMayor": "1200000000"
  },
  {
    "codigo": "1201010000",
    "nombre": "Edificios",
    "ctaMayor": "1201000000"
  },
  {
    "codigo": "1201010001",
    "nombre": "Edificios - Suc. Ferrocarril",
    "ctaMayor": "1201010000"
  },
  {
    "codigo": "1201010002",
    "nombre": "Edificios - Suc. Santa Rosa",
    "ctaMayor": "1201010000"
  },
  {
    "codigo": "1201020000",
    "nombre": "Mobiliario y Equipo de Oficina",
    "ctaMayor": "1201000000"
  },
  {
    "codigo": "1201020001",
    "nombre": "Mobiliario y Equipo de Oficina",
    "ctaMayor": "1201020000"
  },
  {
    "codigo": "1201030000",
    "nombre": "Maquinaria Y Equipo Operacion",
    "ctaMayor": "1201000000"
  },
  {
    "codigo": "1201030001",
    "nombre": "Maquinaria Y Equipo Operacion",
    "ctaMayor": "1201030000"
  },
  {
    "codigo": "1201040000",
    "nombre": "Equipo De Transporte",
    "ctaMayor": "1201000000"
  },
  {
    "codigo": "1201040001",
    "nombre": "Equipo De Transporte",
    "ctaMayor": "1201040000"
  },
  {
    "codigo": "1201050000",
    "nombre": "Equipo De Computo",
    "ctaMayor": "1201000000"
  },
  {
    "codigo": "1201050001",
    "nombre": "Equipo De Computo",
    "ctaMayor": "1201050000"
  },
  {
    "codigo": "1201060000",
    "nombre": "Equipo De Comunicacion",
    "ctaMayor": "1201000000"
  },
  {
    "codigo": "1201060001",
    "nombre": "Equipo De Comunicacion",
    "ctaMayor": "1201060000"
  },
  {
    "codigo": "1201070000",
    "nombre": "Mejoras A Inmuebles Arrendados",
    "ctaMayor": "1201000000"
  },
  {
    "codigo": "1201070001",
    "nombre": "Mejoras A Inmuebles Arrendados",
    "ctaMayor": "1201070000"
  },
  {
    "codigo": "1201080000",
    "nombre": "Otros Activos Fijos",
    "ctaMayor": "1201000000"
  },
  {
    "codigo": "1201080001",
    "nombre": "Otros Activos Fijos",
    "ctaMayor": "1201080000"
  },
  {
    "codigo": "1202000000",
    "nombre": "DEPRECIACION ACUMULADA DE ACTIVOS FIJOS",
    "ctaMayor": "1200000000"
  },
  {
    "codigo": "1202010000",
    "nombre": "Depreciacion Acum. Mobiliario y Equipo de Oficina",
    "ctaMayor": "1202000000"
  },
  {
    "codigo": "1202010001",
    "nombre": "Depreciacion Acum. Mobiliario y Equipo de Oficina",
    "ctaMayor": "1202010000"
  },
  {
    "codigo": "1202020000",
    "nombre": "Depreciacion Acum. Maquinaria Y Equipo Operacion",
    "ctaMayor": "1202000000"
  },
  {
    "codigo": "1202020001",
    "nombre": "Depreciacion Acum. Maquinaria Y Equipo Operacion",
    "ctaMayor": "1202020000"
  },
  {
    "codigo": "1202030000",
    "nombre": "Depreciacion Acum. Equipo De Transporte",
    "ctaMayor": "1202000000"
  },
  {
    "codigo": "1202030001",
    "nombre": "Depreciacion Acum. Equipo De Transporte",
    "ctaMayor": "1202030000"
  },
  {
    "codigo": "1202040000",
    "nombre": "Depreciacion Acum. Equipo De Computo",
    "ctaMayor": "1202000000"
  },
  {
    "codigo": "1202040001",
    "nombre": "Depreciacion Acum. Equipo De Computo",
    "ctaMayor": "1202040000"
  },
  {
    "codigo": "1202050000",
    "nombre": "Depreciacion Acum. Equipo De Comunicacion",
    "ctaMayor": "1202000000"
  },
  {
    "codigo": "1202050001",
    "nombre": "Depreciacion Acum. Equipo De Comunicacion",
    "ctaMayor": "1202050000"
  },
  {
    "codigo": "1202060000",
    "nombre": "Depreciacion Acum. Otros Activos Fijos",
    "ctaMayor": "1202000000"
  },
  {
    "codigo": "1202060001",
    "nombre": "Depreciacion Acum. Otros Activos Fijos",
    "ctaMayor": "1202060000"
  },
  {
    "codigo": "1202070000",
    "nombre": "Depreciacion Acum. Edificios",
    "ctaMayor": "1202000000"
  },
  {
    "codigo": "1202070001",
    "nombre": "Depreciacion Acum. Edificios",
    "ctaMayor": "1202070000"
  },
  {
    "codigo": "1202080000",
    "nombre": "Depreciacion Acum. Mejoras a Inmuebles",
    "ctaMayor": "1202000000"
  },
  {
    "codigo": "1202080001",
    "nombre": "Depreciacion Acum. Mejoras a Inmuebles",
    "ctaMayor": "1202080000"
  },
  {
    "codigo": "1300000000",
    "nombre": "DIFERIDO",
    "ctaMayor": "1000000000"
  },
  {
    "codigo": "1301000000",
    "nombre": "DEPOSITOS EN GARANTIA",
    "ctaMayor": "1300000000"
  },
  {
    "codigo": "1301010000",
    "nombre": "Depositos en Garantia",
    "ctaMayor": "1301000000"
  },
  {
    "codigo": "1301011001",
    "nombre": "Depositos en Garantia - Arrendamiento",
    "ctaMayor": "1301011000"
  },
  {
    "codigo": "1301011002",
    "nombre": "Comision Federal de Electricidad",
    "ctaMayor": "1301011000"
  },
  {
    "codigo": "1301011003",
    "nombre": "Red Efectiva SA de CV",
    "ctaMayor": "1301011000"
  },
  {
    "codigo": "1301011004",
    "nombre": "Administradora de Corresponsales SAPI de CV",
    "ctaMayor": "1301011000"
  },
  {
    "codigo": "1302000000",
    "nombre": "ACTIVOS INTANGIBLES",
    "ctaMayor": "1300000000"
  },
  {
    "codigo": "1302010000",
    "nombre": "Software",
    "ctaMayor": "1302000000"
  },
  {
    "codigo": "1302010001",
    "nombre": "Software",
    "ctaMayor": "1302010000"
  },
  {
    "codigo": "1302020000",
    "nombre": "Marcas",
    "ctaMayor": "1302000000"
  },
  {
    "codigo": "1302020001",
    "nombre": "Marcas",
    "ctaMayor": "1302020000"
  },
  {
    "codigo": "1302030000",
    "nombre": "Patentes",
    "ctaMayor": "1302000000"
  },
  {
    "codigo": "1302030001",
    "nombre": "Patentes",
    "ctaMayor": "1302030000"
  },
  {
    "codigo": "1302040000",
    "nombre": "Otros Intangibles",
    "ctaMayor": "1302000000"
  },
  {
    "codigo": "1302040001",
    "nombre": "Otros Intangibles",
    "ctaMayor": "1302040000"
  },
  {
    "codigo": "1303000000",
    "nombre": "AMORTIZACION ACUMULADA",
    "ctaMayor": "1300000000"
  },
  {
    "codigo": "1303010000",
    "nombre": "Amortizacion Acum. Mejoras A Inmuebles Arrendados",
    "ctaMayor": "1303000000"
  },
  {
    "codigo": "1303010001",
    "nombre": "Amortizacion Acum. Mejoras A Inmuebles Arrendados",
    "ctaMayor": "1303010000"
  },
  {
    "codigo": "2000000000",
    "nombre": "PASIVO",
    "ctaMayor": null
  },
  {
    "codigo": "2100000000",
    "nombre": "A CORTO PLAZO",
    "ctaMayor": "2000000000"
  },
  {
    "codigo": "2101000000",
    "nombre": "PROVEEDORES",
    "ctaMayor": "2100000000"
  },
  {
    "codigo": "2101010000",
    "nombre": "Proveedores Nacionales General 16%",
    "ctaMayor": "2101000000"
  },
  {
    "codigo": "2101010001",
    "nombre": "Proveedores Nacionales General 16%",
    "ctaMayor": "2101010000"
  },
  {
    "codigo": "2101020000",
    "nombre": "Proveedores Nacionales General 0%",
    "ctaMayor": "2101000000"
  },
  {
    "codigo": "2101020001",
    "nombre": "Proveedores Nacionales General 0%",
    "ctaMayor": "2101020000"
  },
  {
    "codigo": "2101030000",
    "nombre": "Proveedores Intercompañias",
    "ctaMayor": "2101000000"
  },
  {
    "codigo": "2101030001",
    "nombre": "Proveedores Intercompañias",
    "ctaMayor": "2101030000"
  },
  {
    "codigo": "2102000000",
    "nombre": "ACREEDORES DIVERSOS",
    "ctaMayor": "2100000000"
  },
  {
    "codigo": "2102010000",
    "nombre": "Acreedores Diversos Gasto",
    "ctaMayor": "2102000000"
  },
  {
    "codigo": "2102010001",
    "nombre": "Acreedores Diversos Gasto General",
    "ctaMayor": "2102010000"
  },
  {
    "codigo": "2102010002",
    "nombre": "Acreedores Diversos Gastos General con Retencion",
    "ctaMayor": "2102010000"
  },
  {
    "codigo": "2102010003",
    "nombre": "Acreedores Diversos Gatos General sin IVA",
    "ctaMayor": "2102010000"
  },
  {
    "codigo": "2102020000",
    "nombre": "Acreedores Diversos Intercompañias",
    "ctaMayor": "2102000000"
  },
  {
    "codigo": "2102020001",
    "nombre": "Acreedores Diversos Intercompañias",
    "ctaMayor": "2102020000"
  },
  {
    "codigo": "2102020002",
    "nombre": "Acreedores Diversos Intercompañias con Retenciones",
    "ctaMayor": "2102020000"
  },
  {
    "codigo": "2103000000",
    "nombre": "ANTICIPOS RECIBIDOS",
    "ctaMayor": "2100000000"
  },
  {
    "codigo": "2103010000",
    "nombre": "Anticipos De Clientes",
    "ctaMayor": "2103000000"
  },
  {
    "codigo": "2103010001",
    "nombre": "Anticipos De Clientes General",
    "ctaMayor": "2103010000"
  },
  {
    "codigo": "2103020000",
    "nombre": "Anticipos Intercompañias",
    "ctaMayor": "2103000000"
  },
  {
    "codigo": "2103020001",
    "nombre": "Anticipos Intercompañias",
    "ctaMayor": "2103020000"
  },
  {
    "codigo": "2103030000",
    "nombre": "Depositos No Identificados",
    "ctaMayor": "2103000000"
  },
  {
    "codigo": "2103030001",
    "nombre": "Depositos No Identificados",
    "ctaMayor": "2103030000"
  },
  {
    "codigo": "2103040000",
    "nombre": "Cobros De Sucursales Por Identificar",
    "ctaMayor": "2103000000"
  },
  {
    "codigo": "2103040001",
    "nombre": "Cobros De Sucursales Por Identificar",
    "ctaMayor": "2103040000"
  },
  {
    "codigo": "2103090000",
    "nombre": "Anticipos Otros",
    "ctaMayor": "2103000000"
  },
  {
    "codigo": "2103090001",
    "nombre": "Anticipos Otros",
    "ctaMayor": "2103090000"
  },
  {
    "codigo": "2103090002",
    "nombre": "Anticipos Otros - Club Tuberos",
    "ctaMayor": "2103090000"
  },
  {
    "codigo": "2104000000",
    "nombre": "IVA TRASLADADO",
    "ctaMayor": "2100000000"
  },
  {
    "codigo": "2104010000",
    "nombre": "IVA Trasladado",
    "ctaMayor": "2104000000"
  },
  {
    "codigo": "2104010001",
    "nombre": "IVA Trasladado",
    "ctaMayor": "2104010000"
  },
  {
    "codigo": "2104010002",
    "nombre": "IVA Trasladado - Anticipos",
    "ctaMayor": "2104010000"
  },
  {
    "codigo": "2105000000",
    "nombre": "IVA POR TRASLADAR",
    "ctaMayor": "2100000000"
  },
  {
    "codigo": "2105010000",
    "nombre": "IVA Por Trasladar",
    "ctaMayor": "2105000000"
  },
  {
    "codigo": "2105010001",
    "nombre": "IVA Por Trasladar",
    "ctaMayor": "2105010000"
  },
  {
    "codigo": "2106000000",
    "nombre": "IMPUESTOS POR PAGAR",
    "ctaMayor": "2100000000"
  },
  {
    "codigo": "2106010000",
    "nombre": "Pagos Provisionales ISR Por Pagar",
    "ctaMayor": "2106000000"
  },
  {
    "codigo": "2106010001",
    "nombre": "Pagos Provisionales ISR Por Pagar",
    "ctaMayor": "2106010000"
  },
  {
    "codigo": "2106020000",
    "nombre": "IVA Por Pagar",
    "ctaMayor": "2106000000"
  },
  {
    "codigo": "2106020001",
    "nombre": "IVA Por Pagar",
    "ctaMayor": "2106020000"
  },
  {
    "codigo": "2106030000",
    "nombre": "Retenciones De ISR",
    "ctaMayor": "2106000000"
  },
  {
    "codigo": "2106030001",
    "nombre": "Retenciones De ISR - Honorarios",
    "ctaMayor": "2106030000"
  },
  {
    "codigo": "2106030002",
    "nombre": "Retenciones De ISR - Arrendamiento",
    "ctaMayor": "2106030000"
  },
  {
    "codigo": "2106030003",
    "nombre": "Retenciones De ISR - Salarios",
    "ctaMayor": "2106030000"
  },
  {
    "codigo": "2106030004",
    "nombre": "Retenciones De ISR - H. Asimilados a Salarios",
    "ctaMayor": "2106030000"
  },
  {
    "codigo": "2106030005",
    "nombre": "Retenciones De ISR - Otros",
    "ctaMayor": "2106030000"
  },
  {
    "codigo": "2106040000",
    "nombre": "Retenciones De IVA",
    "ctaMayor": "2106000000"
  },
  {
    "codigo": "2106040001",
    "nombre": "Retenciones De IVA - Honorarios",
    "ctaMayor": "2106040000"
  },
  {
    "codigo": "2106040002",
    "nombre": "Retenciones De IVA - Arrendamiento",
    "ctaMayor": "2106040000"
  },
  {
    "codigo": "2106040003",
    "nombre": "Retenciones De IVA - Fletes",
    "ctaMayor": "2106040000"
  },
  {
    "codigo": "2106040004",
    "nombre": "Retenciones De IVA - Otros",
    "ctaMayor": "2106040000"
  },
  {
    "codigo": "2106050000",
    "nombre": "Cuotas Obrero Patronales",
    "ctaMayor": "2106000000"
  },
  {
    "codigo": "2106050001",
    "nombre": "Cuotas IMSS",
    "ctaMayor": "2106050000"
  },
  {
    "codigo": "2106050002",
    "nombre": "Aportaciones Infonavit",
    "ctaMayor": "2106050000"
  },
  {
    "codigo": "2106050003",
    "nombre": "Aportaciones SAR",
    "ctaMayor": "2106050000"
  },
  {
    "codigo": "2106060000",
    "nombre": "Impuestos Sobre Nominas",
    "ctaMayor": "2106000000"
  },
  {
    "codigo": "2106060001",
    "nombre": "Impuestos Sobre Nominas",
    "ctaMayor": "2106060000"
  },
  {
    "codigo": "2106070000",
    "nombre": "ISR del Ejercicio",
    "ctaMayor": "2106000000"
  },
  {
    "codigo": "2106070001",
    "nombre": "ISR del Ejercicio",
    "ctaMayor": "2106070000"
  },
  {
    "codigo": "2107000000",
    "nombre": "OTRAS CUENTAS POR PAGAR",
    "ctaMayor": "2100000000"
  },
  {
    "codigo": "2107010000",
    "nombre": "Otras Cuentas Por Pagar",
    "ctaMayor": "2107000000"
  },
  {
    "codigo": "2107010001",
    "nombre": "Retenciones Credito Infonavit",
    "ctaMayor": "2107010000"
  },
  {
    "codigo": "2107010002",
    "nombre": "Retenciones Credito Fonacot",
    "ctaMayor": "2107010000"
  },
  {
    "codigo": "2107010003",
    "nombre": "Retenciones Pension Alimenticia",
    "ctaMayor": "2107010000"
  },
  {
    "codigo": "2107010004",
    "nombre": "Sueldos Por Pagar",
    "ctaMayor": "2107010000"
  },
  {
    "codigo": "2107010005",
    "nombre": "PTU Por Distribuir",
    "ctaMayor": "2107010000"
  },
  {
    "codigo": "2107019998",
    "nombre": "Retenciones Convenio con MTS",
    "ctaMayor": "2107019900"
  },
  {
    "codigo": "2107019999",
    "nombre": "Retenciones Convenio con Terceros",
    "ctaMayor": "2107019900"
  },
  {
    "codigo": "2107020000",
    "nombre": "Cuentas y Documentos por Pagar",
    "ctaMayor": "2107000000"
  },
  {
    "codigo": "2107020001",
    "nombre": "Cuentas y Documentos por Pagar",
    "ctaMayor": "2107020000"
  },
  {
    "codigo": "2107020002",
    "nombre": "Cuentas y Documentos por Pagar Intercompañia",
    "ctaMayor": "2107020000"
  },
  {
    "codigo": "2108000000",
    "nombre": "DEUDA A CORTO PLAZO",
    "ctaMayor": "2100000000"
  },
  {
    "codigo": "2108010000",
    "nombre": "Credito de Linea Revolvente",
    "ctaMayor": "2108000000"
  },
  {
    "codigo": "2108011101",
    "nombre": "Credito de Linea Revolvente",
    "ctaMayor": "2108011100"
  },
  {
    "codigo": "2108020000",
    "nombre": "Credito Bancario A Corto Plazo",
    "ctaMayor": "2108000000"
  },
  {
    "codigo": "2108020001",
    "nombre": "Credito Bancario A Corto Plazo",
    "ctaMayor": "2108020000"
  },
  {
    "codigo": "2108030000",
    "nombre": "Tarjeta de Credito",
    "ctaMayor": "2108000000"
  },
  {
    "codigo": "2108030001",
    "nombre": "Tarjeta de Credito Delt 646180188954200000",
    "ctaMayor": "2108030000"
  },
  {
    "codigo": "2108030002",
    "nombre": "Tarjeta de Credito Clara 2251000200032011",
    "ctaMayor": "2108030000"
  },
  {
    "codigo": "2200000000",
    "nombre": "A LARGO PLAZO",
    "ctaMayor": "2000000000"
  },
  {
    "codigo": "2201000000",
    "nombre": "CUENTAS POR PAGAR A LARGO PLAZO",
    "ctaMayor": "2200000000"
  },
  {
    "codigo": "2201010000",
    "nombre": "Obligaciones Contraídas de Fideicomisos",
    "ctaMayor": "2201000000"
  },
  {
    "codigo": "2201010001",
    "nombre": "FIDE - Contrato: PAEEEMDK09A749841",
    "ctaMayor": "2201010000"
  },
  {
    "codigo": "2201010002",
    "nombre": "FIDE - Contrato: MP-B-6974-19",
    "ctaMayor": "2201010000"
  },
  {
    "codigo": "3000000000",
    "nombre": "CAPITAL",
    "ctaMayor": null
  },
  {
    "codigo": "3100000000",
    "nombre": "CAPITAL CONTABLE",
    "ctaMayor": "3000000000"
  },
  {
    "codigo": "3101000000",
    "nombre": "CAPITAL SOCIAL",
    "ctaMayor": "3100000000"
  },
  {
    "codigo": "3101010000",
    "nombre": "Capital Social Suscrito",
    "ctaMayor": "3101000000"
  },
  {
    "codigo": "3101010001",
    "nombre": "Capital Social Suscrito",
    "ctaMayor": "3101010000"
  },
  {
    "codigo": "3101020000",
    "nombre": "Capital Social Exhibido",
    "ctaMayor": "3101000000"
  },
  {
    "codigo": "3101020001",
    "nombre": "Capital Social Fijo",
    "ctaMayor": "3101020000"
  },
  {
    "codigo": "3101020002",
    "nombre": "Capital Social Variable",
    "ctaMayor": "3101020000"
  },
  {
    "codigo": "3102000000",
    "nombre": "APORT FUTUROS AUMENTOS DE CAPITAL",
    "ctaMayor": "3100000000"
  },
  {
    "codigo": "3102010000",
    "nombre": "Aport Futuros Aumentos De Capital",
    "ctaMayor": "3102000000"
  },
  {
    "codigo": "3102010001",
    "nombre": "Aport Futuros Aumentos De Capital",
    "ctaMayor": "3102010000"
  },
  {
    "codigo": "3103000000",
    "nombre": "RESERVA LEGAL",
    "ctaMayor": "3100000000"
  },
  {
    "codigo": "3103010000",
    "nombre": "Reserva Legal",
    "ctaMayor": "3103000000"
  },
  {
    "codigo": "3103010001",
    "nombre": "Reserva Legal",
    "ctaMayor": "3103010000"
  },
  {
    "codigo": "3104000000",
    "nombre": "RESULTADO DE EJERCICIOS ANTERIORES",
    "ctaMayor": "3100000000"
  },
  {
    "codigo": "3104010000",
    "nombre": "Resultado De Ejercicios Anteriores",
    "ctaMayor": "3104000000"
  },
  {
    "codigo": "3104010001",
    "nombre": "Utilidad De Ejercicios Anteriores",
    "ctaMayor": "3104010000"
  },
  {
    "codigo": "3104010002",
    "nombre": "Perdida De Ejercicios Anteriores",
    "ctaMayor": "3104010000"
  },
  {
    "codigo": "3105000000",
    "nombre": "RESULTADO DEL EJERCICIO",
    "ctaMayor": "3100000000"
  },
  {
    "codigo": "3105010000",
    "nombre": "Resultado Del Ejercicio",
    "ctaMayor": "3105000000"
  },
  {
    "codigo": "3105010001",
    "nombre": "Utilidad del Ejercicio",
    "ctaMayor": "3105010000"
  },
  {
    "codigo": "3105010002",
    "nombre": "Perdida del Ejercicio",
    "ctaMayor": "3105010000"
  },
  {
    "codigo": "4000000000",
    "nombre": "INGRESOS",
    "ctaMayor": null
  },
  {
    "codigo": "4100000000",
    "nombre": "INGRESO POR VENTAS",
    "ctaMayor": "4000000000"
  },
  {
    "codigo": "4100010000",
    "nombre": "Ingresos Por Ventas Contado",
    "ctaMayor": "4100000000"
  },
  {
    "codigo": "4100010001",
    "nombre": "Ingresos Por Ventas Contado Tasa 16%",
    "ctaMayor": "4100010000"
  },
  {
    "codigo": "4100010002",
    "nombre": "Ingresos Por Ventas Contado Tasa 0%",
    "ctaMayor": "4100010000"
  },
  {
    "codigo": "4100010003",
    "nombre": "Ingresos Por Ventas Contado Otros Servicios 16%",
    "ctaMayor": "4100010000"
  },
  {
    "codigo": "4100010004",
    "nombre": "Ingresos Por Ventas Contado Otros Servicios 0%",
    "ctaMayor": "4100010000"
  },
  {
    "codigo": "4100020000",
    "nombre": "Ingresos Por Ventas Credito",
    "ctaMayor": "4100000000"
  },
  {
    "codigo": "4100020001",
    "nombre": "Ingresos Por Ventas Credito Tasa 16%",
    "ctaMayor": "4100020000"
  },
  {
    "codigo": "4100020002",
    "nombre": "Ingresos Por Ventas Credito Tasa 0%",
    "ctaMayor": "4100020000"
  },
  {
    "codigo": "4100020003",
    "nombre": "Ingresos Por Ventas Credito Otros Servicios 16%",
    "ctaMayor": "4100020000"
  },
  {
    "codigo": "4100020004",
    "nombre": "Ingresos Por Ventas Credito Otros Servicios 0%",
    "ctaMayor": "4100020000"
  },
  {
    "codigo": "4100030000",
    "nombre": "Ingresos Por Ventas Intercompañias",
    "ctaMayor": "4100000000"
  },
  {
    "codigo": "4100030001",
    "nombre": "Ingresos Por Ventas Intercompañias Tasa 16%",
    "ctaMayor": "4100030000"
  },
  {
    "codigo": "4100030002",
    "nombre": "Ingresos Por Ventas Intercompañias Tasa 0%",
    "ctaMayor": "4100030000"
  },
  {
    "codigo": "4100030003",
    "nombre": "Ingresos Por Ventas Intercompañias Servicios 16%",
    "ctaMayor": "4100030000"
  },
  {
    "codigo": "4100030004",
    "nombre": "Ingresos Por Ventas Intercompañias Servicios 0%",
    "ctaMayor": "4100030000"
  },
  {
    "codigo": "4200000000",
    "nombre": "DEVOLUCIONES Y DESCUENTOS SOBRE VENTAS",
    "ctaMayor": "4000000000"
  },
  {
    "codigo": "4200010000",
    "nombre": "Devoluciones Sobre Ventas",
    "ctaMayor": "4200000000"
  },
  {
    "codigo": "4200010001",
    "nombre": "Devoluciones Sobre Ventas Tasa 16%",
    "ctaMayor": "4200010000"
  },
  {
    "codigo": "4200010002",
    "nombre": "Devoluciones Sobre Ventas Tasa 0%",
    "ctaMayor": "4200010000"
  },
  {
    "codigo": "4200010003",
    "nombre": "Devoluciones Sobre Ventas Otros Servicios",
    "ctaMayor": "4200010000"
  },
  {
    "codigo": "4200020000",
    "nombre": "Descuentos sobre Ventas",
    "ctaMayor": "4200000000"
  },
  {
    "codigo": "4200020001",
    "nombre": "Descuentos sobre Ventas Tasa 16%",
    "ctaMayor": "4200020000"
  },
  {
    "codigo": "4200020002",
    "nombre": "Descuentos sobre Ventas Tasa 0%",
    "ctaMayor": "4200020000"
  },
  {
    "codigo": "4200030000",
    "nombre": "Devoluciones Sobre Ventas Intercompias",
    "ctaMayor": "4200000000"
  },
  {
    "codigo": "4200030001",
    "nombre": "Devoluciones Sobre Ventas Intercompias Tasa 16%",
    "ctaMayor": "4200030000"
  },
  {
    "codigo": "4200030002",
    "nombre": "Devoluciones Sobre Ventas Intercompias Tasa 0%",
    "ctaMayor": "4200030000"
  },
  {
    "codigo": "4200040000",
    "nombre": "Descuentos sobre Ventas Intercompias",
    "ctaMayor": "4200000000"
  },
  {
    "codigo": "4200040001",
    "nombre": "Descuentos Sobre Ventas Intercompias Tasa 16%",
    "ctaMayor": "4200040000"
  },
  {
    "codigo": "4200040002",
    "nombre": "Descuentos Sobre Ventas Intercompias Tasa 0%",
    "ctaMayor": "4200040000"
  },
  {
    "codigo": "4300000000",
    "nombre": "COSTO DE LO VENDIDO",
    "ctaMayor": "4000000000"
  },
  {
    "codigo": "4300010000",
    "nombre": "Costo De Lo Vendido",
    "ctaMayor": "4300000000"
  },
  {
    "codigo": "4300010001",
    "nombre": "Costo De Lo Vendido",
    "ctaMayor": "4300010000"
  },
  {
    "codigo": "4300020000",
    "nombre": "Costo De Lo Vendido Intercompañias",
    "ctaMayor": "4300000000"
  },
  {
    "codigo": "4300020001",
    "nombre": "Costo De Lo Vendido Intercompañias",
    "ctaMayor": "4300020000"
  },
  {
    "codigo": "5000000000",
    "nombre": "EGRESOS",
    "ctaMayor": null
  },
  {
    "codigo": "5100000000",
    "nombre": "GASTOS DE OPERACIÓN",
    "ctaMayor": "5000000000"
  },
  {
    "codigo": "5101000000",
    "nombre": "RECURSO HUMANO",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5101010000",
    "nombre": "Servicios Contratados",
    "ctaMayor": "5101000000"
  },
  {
    "codigo": "5101010001",
    "nombre": "Servicios Contratados",
    "ctaMayor": "5101010000"
  },
  {
    "codigo": "5101020000",
    "nombre": "Prestaciones Laborales",
    "ctaMayor": "5101000000"
  },
  {
    "codigo": "5101020001",
    "nombre": "Sueldos Y Salarios",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020002",
    "nombre": "IMSS Obrero ND",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020003",
    "nombre": "Cuotas IMSS Patronal",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020004",
    "nombre": "SAR",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020005",
    "nombre": "Infonavit",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020006",
    "nombre": "Retenciones ISR ND",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020007",
    "nombre": "Aguinaldo Exento",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020008",
    "nombre": "Aguinaldo Gravado",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020009",
    "nombre": "Vacaciones Proporcionales",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020010",
    "nombre": "Prima Vacacional Exenta",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020011",
    "nombre": "Prima Vacacional Gravada",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020012",
    "nombre": "Bonos Y Comisiones",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020013",
    "nombre": "Vales De Despensa",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020014",
    "nombre": "Compensacion Por Retiro Gravado",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020015",
    "nombre": "Liquidaciones E Indemnizaciones",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020016",
    "nombre": "Prima de Antiguedad Exenta",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020017",
    "nombre": "Horas Extras Exentas",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020018",
    "nombre": "Horas Extras Gravadas",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020019",
    "nombre": "Premio por Puntualidad",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020020",
    "nombre": "Premio por Asistencia",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020021",
    "nombre": "Prima Dominical Exenta",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020022",
    "nombre": "Prima Dominical Gravada",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020023",
    "nombre": "Dia Festivo Exento",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101020024",
    "nombre": "Dia Festivo Gravado",
    "ctaMayor": "5101020000"
  },
  {
    "codigo": "5101030000",
    "nombre": "Otros Gastos de RH",
    "ctaMayor": "5101000000"
  },
  {
    "codigo": "5101030001",
    "nombre": "Uniformes Y Seguridad",
    "ctaMayor": "5101030000"
  },
  {
    "codigo": "5101030002",
    "nombre": "Capacitacion Y Desarrollo",
    "ctaMayor": "5101030000"
  },
  {
    "codigo": "5101039999",
    "nombre": "Otros Gastos de RH",
    "ctaMayor": "5101039900"
  },
  {
    "codigo": "5101040000",
    "nombre": "Impuestos De Nomina",
    "ctaMayor": "5101000000"
  },
  {
    "codigo": "5101040001",
    "nombre": "Impuestos De Nomina",
    "ctaMayor": "5101040000"
  },
  {
    "codigo": "5101050000",
    "nombre": "PTU",
    "ctaMayor": "5101000000"
  },
  {
    "codigo": "5101050001",
    "nombre": "PTU",
    "ctaMayor": "5101050000"
  },
  {
    "codigo": "5102000000",
    "nombre": "HONORARIOS",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5102010000",
    "nombre": "Honorarios",
    "ctaMayor": "5102000000"
  },
  {
    "codigo": "5102010001",
    "nombre": "Honorarios Legales",
    "ctaMayor": "5102010000"
  },
  {
    "codigo": "5102010002",
    "nombre": "Honorarios Contables",
    "ctaMayor": "5102010000"
  },
  {
    "codigo": "5102010003",
    "nombre": "Honorarios Fiscales",
    "ctaMayor": "5102010000"
  },
  {
    "codigo": "5102010004",
    "nombre": "Honorarios Asimilados a Salarios",
    "ctaMayor": "5102010000"
  },
  {
    "codigo": "5102020000",
    "nombre": "Servicios Profesionales",
    "ctaMayor": "5102000000"
  },
  {
    "codigo": "5102020001",
    "nombre": "Servicios Profesionales",
    "ctaMayor": "5102020000"
  },
  {
    "codigo": "5103000000",
    "nombre": "CONSULTORIAS",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5103010000",
    "nombre": "Consultorias",
    "ctaMayor": "5103000000"
  },
  {
    "codigo": "5103010001",
    "nombre": "Consultorias Legales",
    "ctaMayor": "5103010000"
  },
  {
    "codigo": "5103010002",
    "nombre": "Consultorias Contables",
    "ctaMayor": "5103010000"
  },
  {
    "codigo": "5103010003",
    "nombre": "Consultorias Fiscales",
    "ctaMayor": "5103010000"
  },
  {
    "codigo": "5103019999",
    "nombre": "Otros Gastos de Consultoria",
    "ctaMayor": "5103019900"
  },
  {
    "codigo": "5103020000",
    "nombre": "Servicios Profesionales Intercompañia",
    "ctaMayor": "5103000000"
  },
  {
    "codigo": "5103020001",
    "nombre": "Servicios Profesionales Intercompañia",
    "ctaMayor": "5103020000"
  },
  {
    "codigo": "5104000000",
    "nombre": "GASTOS DE TRASLADO Y VIAJE",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5104010000",
    "nombre": "Hospedaje",
    "ctaMayor": "5104000000"
  },
  {
    "codigo": "5104010001",
    "nombre": "Hospedaje",
    "ctaMayor": "5104010000"
  },
  {
    "codigo": "5104020000",
    "nombre": "Alimentos",
    "ctaMayor": "5104000000"
  },
  {
    "codigo": "5104020001",
    "nombre": "Alimentos",
    "ctaMayor": "5104020000"
  },
  {
    "codigo": "5104030000",
    "nombre": "Combustibles",
    "ctaMayor": "5104000000"
  },
  {
    "codigo": "5104030001",
    "nombre": "Combustibles",
    "ctaMayor": "5104030000"
  },
  {
    "codigo": "5104040000",
    "nombre": "Peajes",
    "ctaMayor": "5104000000"
  },
  {
    "codigo": "5104040001",
    "nombre": "Peajes",
    "ctaMayor": "5104040000"
  },
  {
    "codigo": "5104050000",
    "nombre": "Pasajes",
    "ctaMayor": "5104000000"
  },
  {
    "codigo": "5104050001",
    "nombre": "Pasajes",
    "ctaMayor": "5104050000"
  },
  {
    "codigo": "5104060000",
    "nombre": "Grua, Custodia y Pension",
    "ctaMayor": "5104000000"
  },
  {
    "codigo": "5104060001",
    "nombre": "Grua y Arrastre",
    "ctaMayor": "5104060000"
  },
  {
    "codigo": "5104060002",
    "nombre": "Custodia y Pension",
    "ctaMayor": "5104060000"
  },
  {
    "codigo": "5104990000",
    "nombre": "Otros Gastos De Traslado Y Viaje",
    "ctaMayor": "5104000000"
  },
  {
    "codigo": "5104990001",
    "nombre": "Otros Gastos De Traslado Y Viaje",
    "ctaMayor": "5104990000"
  },
  {
    "codigo": "5105000000",
    "nombre": "GASTOS DE DISTRIBUCION Y VENTAS",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5105010000",
    "nombre": "Fletes",
    "ctaMayor": "5105000000"
  },
  {
    "codigo": "5105010001",
    "nombre": "Fletes Sobre Compras",
    "ctaMayor": "5105010000"
  },
  {
    "codigo": "5105010002",
    "nombre": "Fletes Sobre Ventas",
    "ctaMayor": "5105010000"
  },
  {
    "codigo": "5105020000",
    "nombre": "Empaques, Seguros Y Maniobras",
    "ctaMayor": "5105000000"
  },
  {
    "codigo": "5105020001",
    "nombre": "Embalajes Y Empaques",
    "ctaMayor": "5105020000"
  },
  {
    "codigo": "5105020002",
    "nombre": "Seguros Y Maniobras Compras",
    "ctaMayor": "5105020000"
  },
  {
    "codigo": "5105020003",
    "nombre": "Seguros Y Maniobras Ventas",
    "ctaMayor": "5105020000"
  },
  {
    "codigo": "5105030000",
    "nombre": "Comisiones De Venta",
    "ctaMayor": "5105000000"
  },
  {
    "codigo": "5105030001",
    "nombre": "Comisiones De Venta",
    "ctaMayor": "5105030000"
  },
  {
    "codigo": "5105990000",
    "nombre": "Otros Gastos De Dist. Y Ventas",
    "ctaMayor": "5105000000"
  },
  {
    "codigo": "5105990001",
    "nombre": "Otros Gastos De Dist. Y Ventas",
    "ctaMayor": "5105990000"
  },
  {
    "codigo": "5106000000",
    "nombre": "GASTOS OFICINA",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5106010000",
    "nombre": "Electricidad",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106010001",
    "nombre": "Electricidad",
    "ctaMayor": "5106010000"
  },
  {
    "codigo": "5106020000",
    "nombre": "Telefono Y Celulares",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106020001",
    "nombre": "Telefono Y Celulares",
    "ctaMayor": "5106020000"
  },
  {
    "codigo": "5106030000",
    "nombre": "Traslado De Valores",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106030001",
    "nombre": "Traslado De Valores",
    "ctaMayor": "5106030000"
  },
  {
    "codigo": "5106040000",
    "nombre": "Validacion De Cheques",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106040001",
    "nombre": "Validacion De Cheques",
    "ctaMayor": "5106040000"
  },
  {
    "codigo": "5106050000",
    "nombre": "Seguridad Y Vigilancia",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106050001",
    "nombre": "Seguridad Y Vigilancia",
    "ctaMayor": "5106050000"
  },
  {
    "codigo": "5106060000",
    "nombre": "Papeleria, Consumibles, Accesorios",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106060001",
    "nombre": "Papeleria, Consumibles, Accesorios",
    "ctaMayor": "5106060000"
  },
  {
    "codigo": "5106070000",
    "nombre": "Mensajeria Y Paqueteria",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106070001",
    "nombre": "Mensajeria Y Paqueteria",
    "ctaMayor": "5106070000"
  },
  {
    "codigo": "5106080000",
    "nombre": "Cuotas Y Suscripciones",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106080001",
    "nombre": "Cuotas Y Suscripciones",
    "ctaMayor": "5106080000"
  },
  {
    "codigo": "5106090000",
    "nombre": "Articulos de Limpieza",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106090001",
    "nombre": "Articulos de Limpieza",
    "ctaMayor": "5106090000"
  },
  {
    "codigo": "5106100000",
    "nombre": "Botiquin y Articulos de Curacion",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5106100001",
    "nombre": "Botiquin y Articulos de Curacion",
    "ctaMayor": "5106100000"
  },
  {
    "codigo": "5106100002",
    "nombre": "Producto Quimicos, Sanitizantes y Solventes",
    "ctaMayor": "5106100000"
  },
  {
    "codigo": "5106100003",
    "nombre": "Diagnostico Medico y Pruebas",
    "ctaMayor": "5106100000"
  },
  {
    "codigo": "5106110000",
    "nombre": "Mobiliario y Articulos de Oficina",
    "ctaMayor": "5106000000"
  },
  {
    "codigo": "5106110001",
    "nombre": "Mobiliario y Articulos de Oficina",
    "ctaMayor": "5106110000"
  },
  {
    "codigo": "5107000000",
    "nombre": "COMUNICACIONES Y SISTEMAS",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5107010000",
    "nombre": "Tecnologia",
    "ctaMayor": "5107000000"
  },
  {
    "codigo": "5107010001",
    "nombre": "Tecnologia",
    "ctaMayor": "5107010000"
  },
  {
    "codigo": "5107020000",
    "nombre": "Licencias Y Uso De Software",
    "ctaMayor": "5107000000"
  },
  {
    "codigo": "5107020001",
    "nombre": "Licencias Y Uso De Software",
    "ctaMayor": "5107020000"
  },
  {
    "codigo": "5107030000",
    "nombre": "Comunicaciones",
    "ctaMayor": "5107000000"
  },
  {
    "codigo": "5107030001",
    "nombre": "Comunicaciones",
    "ctaMayor": "5107030000"
  },
  {
    "codigo": "5107040000",
    "nombre": "Servicios Tecnologicos",
    "ctaMayor": "5107000000"
  },
  {
    "codigo": "5107040001",
    "nombre": "Servicios Tecnologicos",
    "ctaMayor": "5107040000"
  },
  {
    "codigo": "5107050000",
    "nombre": "Consumibles y Accesorios",
    "ctaMayor": "5107000000"
  },
  {
    "codigo": "5107050001",
    "nombre": "Consumibles y Accesorios",
    "ctaMayor": "5107050000"
  },
  {
    "codigo": "5107990000",
    "nombre": "Otros Gastos De Comunicaciones Y Sistemas",
    "ctaMayor": "5107000000"
  },
  {
    "codigo": "5107990001",
    "nombre": "Otros Gastos De Comunicaciones Y Sistemas",
    "ctaMayor": "5107990000"
  },
  {
    "codigo": "5108000000",
    "nombre": "ARRENDAMIENTO DE BIENES Y SERVICIOS",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5108010000",
    "nombre": "Arrendamiento Terreno",
    "ctaMayor": "5108000000"
  },
  {
    "codigo": "5108010001",
    "nombre": "Arrendamiento Terreno PM",
    "ctaMayor": "5108010000"
  },
  {
    "codigo": "5108010002",
    "nombre": "Arrendamiento Terreno PF",
    "ctaMayor": "5108010000"
  },
  {
    "codigo": "5108020000",
    "nombre": "Arrendamiento Local",
    "ctaMayor": "5108000000"
  },
  {
    "codigo": "5108020001",
    "nombre": "Arrendamiento Local PM",
    "ctaMayor": "5108020000"
  },
  {
    "codigo": "5108020002",
    "nombre": "Arrendamiento Local PF",
    "ctaMayor": "5108020000"
  },
  {
    "codigo": "5108030000",
    "nombre": "Arrendamiento Mobiliario y Equipo de Oficina",
    "ctaMayor": "5108000000"
  },
  {
    "codigo": "5108030001",
    "nombre": "Arrendamiento Mobiliario y Equipo de Oficina PM",
    "ctaMayor": "5108030000"
  },
  {
    "codigo": "5108030002",
    "nombre": "Arrendamiento Mobiliario y Equipo de Oficina PF",
    "ctaMayor": "5108030000"
  },
  {
    "codigo": "5108040000",
    "nombre": "Arrendamiento Maquinaria y Equipo de Operacion",
    "ctaMayor": "5108000000"
  },
  {
    "codigo": "5108040001",
    "nombre": "Arrendamiento Maquinaria y Equipo de Operacion PM",
    "ctaMayor": "5108040000"
  },
  {
    "codigo": "5108040002",
    "nombre": "Arrendamiento Maquinaria y Equipo de Operacion PF",
    "ctaMayor": "5108040000"
  },
  {
    "codigo": "5108050000",
    "nombre": "Arrendamiento Equipo De Transporte",
    "ctaMayor": "5108000000"
  },
  {
    "codigo": "5108050001",
    "nombre": "Arrendamiento Equipo De Transporte PM",
    "ctaMayor": "5108050000"
  },
  {
    "codigo": "5108050002",
    "nombre": "Arrendamiento Equipo De Transporte PF",
    "ctaMayor": "5108050000"
  },
  {
    "codigo": "5108060000",
    "nombre": "Arrendamiento Equipo De Computo",
    "ctaMayor": "5108000000"
  },
  {
    "codigo": "5108060001",
    "nombre": "Arrendamiento Equipo De Computo PM",
    "ctaMayor": "5108060000"
  },
  {
    "codigo": "5108060002",
    "nombre": "Arrendamiento Equipo De Computo PF",
    "ctaMayor": "5108060000"
  },
  {
    "codigo": "5108070000",
    "nombre": "Arrendamiento Equipo De Comunicaciones",
    "ctaMayor": "5108000000"
  },
  {
    "codigo": "5108070001",
    "nombre": "Arrendamiento Equipo De Comunicaciones PM",
    "ctaMayor": "5108070000"
  },
  {
    "codigo": "5108070002",
    "nombre": "Arrendamiento Equipo De Comunicaciones PF",
    "ctaMayor": "5108070000"
  },
  {
    "codigo": "5108990000",
    "nombre": "Arrendamiento Otros Activos",
    "ctaMayor": "5108000000"
  },
  {
    "codigo": "5108990001",
    "nombre": "Arrendamiento Otros Activos PM",
    "ctaMayor": "5108990000"
  },
  {
    "codigo": "5108990002",
    "nombre": "Arrendamiento Otros Activos PF",
    "ctaMayor": "5108990000"
  },
  {
    "codigo": "5109000000",
    "nombre": "MANTENIMIENTO",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5109010000",
    "nombre": "Mantenimiento Inmuebles Arrendados",
    "ctaMayor": "5109000000"
  },
  {
    "codigo": "5109010001",
    "nombre": "Mantenimiento Inmuebles Arrendados",
    "ctaMayor": "5109010000"
  },
  {
    "codigo": "5109020000",
    "nombre": "Mantenimiento Mobiliario y Equipo de Oficina",
    "ctaMayor": "5109000000"
  },
  {
    "codigo": "5109020001",
    "nombre": "Mantenimiento Mobiliario y Equipo de Oficina",
    "ctaMayor": "5109020000"
  },
  {
    "codigo": "5109030000",
    "nombre": "Mantenimiento Maquinaria y Equipo de Operacion",
    "ctaMayor": "5109000000"
  },
  {
    "codigo": "5109030001",
    "nombre": "Mantenimiento Maquinaria y Equipo de Operacion",
    "ctaMayor": "5109030000"
  },
  {
    "codigo": "5109040000",
    "nombre": "Mantenimiento Equipo De Transporte",
    "ctaMayor": "5109000000"
  },
  {
    "codigo": "5109040001",
    "nombre": "Mantenimiento Equipo De Transporte",
    "ctaMayor": "5109040000"
  },
  {
    "codigo": "5109050000",
    "nombre": "Mantenimiento Equipo De Computo",
    "ctaMayor": "5109000000"
  },
  {
    "codigo": "5109050001",
    "nombre": "Mantenimiento Equipo De Computo",
    "ctaMayor": "5109050000"
  },
  {
    "codigo": "5109060000",
    "nombre": "Mantenimiento Equipo De Comunicaciones",
    "ctaMayor": "5109000000"
  },
  {
    "codigo": "5109060001",
    "nombre": "Mantenimiento Equipo De Comunicaciones",
    "ctaMayor": "5109060000"
  },
  {
    "codigo": "5109990000",
    "nombre": "Mantenimiento Otros Activos",
    "ctaMayor": "5109000000"
  },
  {
    "codigo": "5109990001",
    "nombre": "Mantenimiento Otros Activos",
    "ctaMayor": "5109990000"
  },
  {
    "codigo": "5110000000",
    "nombre": "SEGUROS Y FIANZAS",
    "ctaMayor": "5000000000"
  },
  {
    "codigo": "5110010000",
    "nombre": "Seguros",
    "ctaMayor": "5110000000"
  },
  {
    "codigo": "5110010001",
    "nombre": "Seguros",
    "ctaMayor": "5110010000"
  },
  {
    "codigo": "5110020000",
    "nombre": "Fianzas",
    "ctaMayor": "5110000000"
  },
  {
    "codigo": "5110020001",
    "nombre": "Fianzas",
    "ctaMayor": "5110020000"
  },
  {
    "codigo": "5111000000",
    "nombre": "MERCADOTECNIA",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5111010000",
    "nombre": "Publicidad",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111010001",
    "nombre": "Publicidad",
    "ctaMayor": "5111010000"
  },
  {
    "codigo": "5111010002",
    "nombre": "Propaganda",
    "ctaMayor": "5111010000"
  },
  {
    "codigo": "5111020000",
    "nombre": "Investigacion De Mercados",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111020001",
    "nombre": "Investigacion De Mercados",
    "ctaMayor": "5111020000"
  },
  {
    "codigo": "5111030000",
    "nombre": "Patrocinios",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111030001",
    "nombre": "Patrocinios",
    "ctaMayor": "5111030000"
  },
  {
    "codigo": "5111040000",
    "nombre": "Promocionales",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111040001",
    "nombre": "Promocionales",
    "ctaMayor": "5111040000"
  },
  {
    "codigo": "5111050000",
    "nombre": "Catalogos",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111050001",
    "nombre": "Catalogos",
    "ctaMayor": "5111050000"
  },
  {
    "codigo": "5111060000",
    "nombre": "Atencion A Clientes",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111060001",
    "nombre": "Atencion A Clientes",
    "ctaMayor": "5111060000"
  },
  {
    "codigo": "5111070000",
    "nombre": "Impresiones y Graficos",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111070001",
    "nombre": "Impresiones y Lonas",
    "ctaMayor": "5111070000"
  },
  {
    "codigo": "5111070002",
    "nombre": "Rotulación vehículos",
    "ctaMayor": "5111070000"
  },
  {
    "codigo": "5111070003",
    "nombre": "Anuncios luminosos",
    "ctaMayor": "5111070000"
  },
  {
    "codigo": "5111070004",
    "nombre": "Otros rotulos y anuncios",
    "ctaMayor": "5111070000"
  },
  {
    "codigo": "5111070005",
    "nombre": "Serigrafía y Bordados",
    "ctaMayor": "5111070000"
  },
  {
    "codigo": "5111080000",
    "nombre": "Medios de Comunicación",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111080001",
    "nombre": "Radio",
    "ctaMayor": "5111080000"
  },
  {
    "codigo": "5111080002",
    "nombre": "Bardas",
    "ctaMayor": "5111080000"
  },
  {
    "codigo": "5111080003",
    "nombre": "Medallones",
    "ctaMayor": "5111080000"
  },
  {
    "codigo": "5111080004",
    "nombre": "Espectaculares",
    "ctaMayor": "5111080000"
  },
  {
    "codigo": "5111080005",
    "nombre": "Perifoneo",
    "ctaMayor": "5111080000"
  },
  {
    "codigo": "5111080006",
    "nombre": "Periodico",
    "ctaMayor": "5111080000"
  },
  {
    "codigo": "5111080007",
    "nombre": "Revistas y publicaciones",
    "ctaMayor": "5111080000"
  },
  {
    "codigo": "5111090000",
    "nombre": "Internet y redes",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111090001",
    "nombre": "SMS",
    "ctaMayor": "5111090000"
  },
  {
    "codigo": "5111090002",
    "nombre": "Facebook",
    "ctaMayor": "5111090000"
  },
  {
    "codigo": "5111090003",
    "nombre": "Google",
    "ctaMayor": "5111090000"
  },
  {
    "codigo": "5111090004",
    "nombre": "Sitios Web",
    "ctaMayor": "5111090000"
  },
  {
    "codigo": "5111099999",
    "nombre": "Otros Gastos de Publicidad en Internet y Redes",
    "ctaMayor": "5111099900"
  },
  {
    "codigo": "5111100000",
    "nombre": "Eventos y Apoyo a Clientes",
    "ctaMayor": "5110000000"
  },
  {
    "codigo": "5111100001",
    "nombre": "Rifas y Concursos",
    "ctaMayor": "5111100000"
  },
  {
    "codigo": "5111100002",
    "nombre": "Eventos",
    "ctaMayor": "5111100000"
  },
  {
    "codigo": "5111100003",
    "nombre": "Patrocinios",
    "ctaMayor": "5111100000"
  },
  {
    "codigo": "5111100004",
    "nombre": "Activaciones",
    "ctaMayor": "5111100000"
  },
  {
    "codigo": "5111110000",
    "nombre": "Programa de Lealtad",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111110001",
    "nombre": "Tarjetas",
    "ctaMayor": "5111110000"
  },
  {
    "codigo": "5111110002",
    "nombre": "Eventos Programa de Lealtad",
    "ctaMayor": "5111110000"
  },
  {
    "codigo": "5111110003",
    "nombre": "Promocionales / Regalos Programa de Lealtad",
    "ctaMayor": "5111110000"
  },
  {
    "codigo": "5111119999",
    "nombre": "Otros Gastos Programa de Lealtad",
    "ctaMayor": "5111119900"
  },
  {
    "codigo": "5111120000",
    "nombre": "Promocionales",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111120001",
    "nombre": "Promocionales Normales",
    "ctaMayor": "5111120000"
  },
  {
    "codigo": "5111120003",
    "nombre": "Promocionales Fin de Año",
    "ctaMayor": "5111120000"
  },
  {
    "codigo": "5111130000",
    "nombre": "Diseño, Produccion y Subcontratacion",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111130001",
    "nombre": "Produccion de Fotografia",
    "ctaMayor": "5111130000"
  },
  {
    "codigo": "5111130002",
    "nombre": "Diseño Grafico",
    "ctaMayor": "5111130000"
  },
  {
    "codigo": "5111130003",
    "nombre": "Produccion de Audio",
    "ctaMayor": "5111130000"
  },
  {
    "codigo": "5111130004",
    "nombre": "Personal de Apoyo",
    "ctaMayor": "5111130000"
  },
  {
    "codigo": "5111140000",
    "nombre": "Uniformes",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111140001",
    "nombre": "Uniformes",
    "ctaMayor": "5111140000"
  },
  {
    "codigo": "5111150000",
    "nombre": "Otros Materiales para Mercadotecnia",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111150001",
    "nombre": "Otros Materiales para Mercadotecnia",
    "ctaMayor": "5111150000"
  },
  {
    "codigo": "5111160000",
    "nombre": "Estudios y Encuestas",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111160001",
    "nombre": "Estudios de Mercado",
    "ctaMayor": "5111160000"
  },
  {
    "codigo": "5111160002",
    "nombre": "Encuestas",
    "ctaMayor": "5111160000"
  },
  {
    "codigo": "5111170000",
    "nombre": "Apoyos para Mercadotecnia",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111170001",
    "nombre": "Apoyos para Mercadotecnia",
    "ctaMayor": "5111170000"
  },
  {
    "codigo": "5111990000",
    "nombre": "Otros Gastos De Mercadotecnia",
    "ctaMayor": "5111000000"
  },
  {
    "codigo": "5111990001",
    "nombre": "Otros Gastos De Mercadotecnia",
    "ctaMayor": "5111990000"
  },
  {
    "codigo": "5111990002",
    "nombre": "Articulos Navideños",
    "ctaMayor": "5111990000"
  },
  {
    "codigo": "5112000000",
    "nombre": "IMPUESTOS Y DERECHOS",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5112010000",
    "nombre": "Tenencia y Revista",
    "ctaMayor": "5112000000"
  },
  {
    "codigo": "5112010001",
    "nombre": "Tenencia",
    "ctaMayor": "5112010000"
  },
  {
    "codigo": "5112010002",
    "nombre": "Revista Fisico-Mecanica",
    "ctaMayor": "5112010000"
  },
  {
    "codigo": "5112010003",
    "nombre": "Verificacion De Contaminantes",
    "ctaMayor": "5112010000"
  },
  {
    "codigo": "5112020000",
    "nombre": "Predial",
    "ctaMayor": "5112000000"
  },
  {
    "codigo": "5112020001",
    "nombre": "Predial",
    "ctaMayor": "5112020000"
  },
  {
    "codigo": "5112030000",
    "nombre": "Licencias Y Permisos",
    "ctaMayor": "5112000000"
  },
  {
    "codigo": "5112030001",
    "nombre": "Licencias Y Permisos",
    "ctaMayor": "5112030000"
  },
  {
    "codigo": "5112040000",
    "nombre": "Multas, Actualizaciones, Recargos",
    "ctaMayor": "5112000000"
  },
  {
    "codigo": "5112040001",
    "nombre": "Multas, Actualizaciones",
    "ctaMayor": "5112040000"
  },
  {
    "codigo": "5112040002",
    "nombre": "Recargos",
    "ctaMayor": "5112040000"
  },
  {
    "codigo": "5112050000",
    "nombre": "ISH - Impuesto Sobre Hospedaje",
    "ctaMayor": "5112000000"
  },
  {
    "codigo": "5112050001",
    "nombre": "ISH - Impuesto Sobre Hospedaje",
    "ctaMayor": "5112050000"
  },
  {
    "codigo": "5112060000",
    "nombre": "IEPS - Impuesto Especial Sobre Produc y Serv",
    "ctaMayor": "5112000000"
  },
  {
    "codigo": "5112060001",
    "nombre": "IEPS - Impuesto Especial Sobre Produc y Serv",
    "ctaMayor": "5112060000"
  },
  {
    "codigo": "5112070000",
    "nombre": "DAP - Derecho al Alumbrado Publico",
    "ctaMayor": "5112000000"
  },
  {
    "codigo": "5112070001",
    "nombre": "DAP - Derecho al Alumbrado Publico",
    "ctaMayor": "5112070000"
  },
  {
    "codigo": "5112990000",
    "nombre": "Otros Impuestos Y Derechos",
    "ctaMayor": "5112000000"
  },
  {
    "codigo": "5112990001",
    "nombre": "Otros Impuestos Y Derechos",
    "ctaMayor": "5112990000"
  },
  {
    "codigo": "5113000000",
    "nombre": "OTROS GASTOS OFICINA - SIN IVA",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5113010000",
    "nombre": "Agua",
    "ctaMayor": "5113000000"
  },
  {
    "codigo": "5113010001",
    "nombre": "Agua",
    "ctaMayor": "5113010000"
  },
  {
    "codigo": "5113020000",
    "nombre": "Donativos",
    "ctaMayor": "5113000000"
  },
  {
    "codigo": "5113020001",
    "nombre": "Donativos",
    "ctaMayor": "5113020000"
  },
  {
    "codigo": "5113990000",
    "nombre": "Otros Gastos de Oficina",
    "ctaMayor": "5113000000"
  },
  {
    "codigo": "5113990001",
    "nombre": "Papeleria, Consumibles, Accesorios sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990002",
    "nombre": "Cuotas Y Suscripciones sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990003",
    "nombre": "Botiquin y Articulos de Curacion sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990004",
    "nombre": "Atencion a Clientes sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990005",
    "nombre": "Seguros sin IVA - Deducibles",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990006",
    "nombre": "Articulos de Limpieza sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990007",
    "nombre": "Cuotas y Suscripciones sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990008",
    "nombre": "Otros Gastos de Comunicaciones y Sistemas sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990009",
    "nombre": "Otros Gastos De Mercadotecnia sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990010",
    "nombre": "Alimentos sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990011",
    "nombre": "Otros Gastos de RH sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5113990012",
    "nombre": "Articulos Navideños sin IVA",
    "ctaMayor": "5113990000"
  },
  {
    "codigo": "5114000000",
    "nombre": "OTROS GASTOS DE OPERACION",
    "ctaMayor": "5100000000"
  },
  {
    "codigo": "5114010000",
    "nombre": "Otros Gastos De Operacion",
    "ctaMayor": "5114000000"
  },
  {
    "codigo": "5114010001",
    "nombre": "Gastos Por Robo de Efectivo No Recuperable",
    "ctaMayor": "5114010000"
  },
  {
    "codigo": "5114010002",
    "nombre": "Gastos Por Robo de Material sobre Compras",
    "ctaMayor": "5114010000"
  },
  {
    "codigo": "5114010003",
    "nombre": "Gastos Por Robo de Material sobre Ventas",
    "ctaMayor": "5114010000"
  },
  {
    "codigo": "5114019998",
    "nombre": "Otros Gastos De Operacion",
    "ctaMayor": "5114019900"
  },
  {
    "codigo": "5114019999",
    "nombre": "Gastos No Deducibles",
    "ctaMayor": "5114019900"
  },
  {
    "codigo": "5114020000",
    "nombre": "Cuentas Incobrables",
    "ctaMayor": "5114000000"
  },
  {
    "codigo": "5114020001",
    "nombre": "Cuentas Incobrables",
    "ctaMayor": "5114020000"
  },
  {
    "codigo": "5200000000",
    "nombre": "GASTO Y PRODUCTO FINANCIERO",
    "ctaMayor": "5000000000"
  },
  {
    "codigo": "5201000000",
    "nombre": "GASTOS FINANCIEROS",
    "ctaMayor": "5200000000"
  },
  {
    "codigo": "5201010000",
    "nombre": "Intereses A Pagar",
    "ctaMayor": "5201000000"
  },
  {
    "codigo": "5201010001",
    "nombre": "Intereses A Pagar",
    "ctaMayor": "5201010000"
  },
  {
    "codigo": "5201020000",
    "nombre": "Comisiones Bancarias",
    "ctaMayor": "5201000000"
  },
  {
    "codigo": "5201020001",
    "nombre": "Comisiones Bancarias",
    "ctaMayor": "5201020000"
  },
  {
    "codigo": "5201030000",
    "nombre": "Comisiones Venta con Tarjeta",
    "ctaMayor": "5201000000"
  },
  {
    "codigo": "5201030001",
    "nombre": "Comisiones Venta con Tarjeta",
    "ctaMayor": "5201030000"
  },
  {
    "codigo": "5201040000",
    "nombre": "Perdida Cambiaria",
    "ctaMayor": "5201000000"
  },
  {
    "codigo": "5201040001",
    "nombre": "Perdida Cambiaria",
    "ctaMayor": "5201040000"
  },
  {
    "codigo": "5202000000",
    "nombre": "OTROS GASTOS FINANCIEROS",
    "ctaMayor": "5200000000"
  },
  {
    "codigo": "5202010000",
    "nombre": "Desctos Pronto Pago Otorgados",
    "ctaMayor": "5202000000"
  },
  {
    "codigo": "5202010001",
    "nombre": "Desctos Pronto Pago Otorgados",
    "ctaMayor": "5202010000"
  },
  {
    "codigo": "5202020000",
    "nombre": "Perdida Por Revaluacion Activos Y Pasivos  Financ",
    "ctaMayor": "5202000000"
  },
  {
    "codigo": "5202020001",
    "nombre": "Perdida Por Revaluacion Activos Y Pasivos  Financ",
    "ctaMayor": "5202020000"
  },
  {
    "codigo": "5202030000",
    "nombre": "Perdida en Venta de Activo",
    "ctaMayor": "5202000000"
  },
  {
    "codigo": "5202030001",
    "nombre": "Perdida en Venta o Baja de Activo",
    "ctaMayor": "5202030000"
  },
  {
    "codigo": "5202040000",
    "nombre": "Descuentos No Procedentes de Proveedores",
    "ctaMayor": "5202000000"
  },
  {
    "codigo": "5202040001",
    "nombre": "Descuentos No Procedentes de Proveedores",
    "ctaMayor": "5202040000"
  },
  {
    "codigo": "5202990000",
    "nombre": "Otros Gastos",
    "ctaMayor": "5202000000"
  },
  {
    "codigo": "5202990001",
    "nombre": "Otros Gastos",
    "ctaMayor": "5202990000"
  },
  {
    "codigo": "5202990002",
    "nombre": "Comisiones Por Operaciones Comerciales",
    "ctaMayor": "5202990000"
  },
  {
    "codigo": "5203000000",
    "nombre": "PRODUCTOS FINANCIEROS",
    "ctaMayor": "5200000000"
  },
  {
    "codigo": "5203010000",
    "nombre": "Intereses Ganados",
    "ctaMayor": "5203000000"
  },
  {
    "codigo": "5203010001",
    "nombre": "Intereses Ganados",
    "ctaMayor": "5203010000"
  },
  {
    "codigo": "5203020000",
    "nombre": "Ganancia Cambiaria",
    "ctaMayor": "5203000000"
  },
  {
    "codigo": "5203020001",
    "nombre": "Ganancia Cambiaria",
    "ctaMayor": "5203020000"
  },
  {
    "codigo": "5204000000",
    "nombre": "OTROS PRODUCTOS FINANCIEROS",
    "ctaMayor": "5200000000"
  },
  {
    "codigo": "5204010000",
    "nombre": "Desctos Pronto Pago Recibidos",
    "ctaMayor": "5204000000"
  },
  {
    "codigo": "5204010001",
    "nombre": "Desctos Pronto Pago Recibidos",
    "ctaMayor": "5204010000"
  },
  {
    "codigo": "5204010002",
    "nombre": "Desctos Pronto Pago Recibidos 0%",
    "ctaMayor": "5204010000"
  },
  {
    "codigo": "5204020000",
    "nombre": "Ganancia Por Revaluacion Activos Y Pasivos Financ",
    "ctaMayor": "5204000000"
  },
  {
    "codigo": "5204020001",
    "nombre": "Ganancia Por Revaluacion Activos Y Pasivos  Financ",
    "ctaMayor": "5204020000"
  },
  {
    "codigo": "5204030000",
    "nombre": "Utilidad en Venta de Activo",
    "ctaMayor": "5204000000"
  },
  {
    "codigo": "5204030001",
    "nombre": "Utilidad en Venta de Activo - Transporte",
    "ctaMayor": "5204030000"
  },
  {
    "codigo": "5204040000",
    "nombre": "Descuentos y Bonificaciones sobre Compras",
    "ctaMayor": "5204000000"
  },
  {
    "codigo": "5204040001",
    "nombre": "Descuentos y Bonificaciones sobre Compras",
    "ctaMayor": "5204040000"
  },
  {
    "codigo": "5204040002",
    "nombre": "Descuentos y Bonificaciones sobre Compras 0%",
    "ctaMayor": "5204040000"
  },
  {
    "codigo": "5204050000",
    "nombre": "Descuentos por Apoyos Publicitarios",
    "ctaMayor": "5204000000"
  },
  {
    "codigo": "5204050001",
    "nombre": "Descuentos por Apoyos Publicitarios",
    "ctaMayor": "5204050000"
  },
  {
    "codigo": "5204060000",
    "nombre": "Ingresos por Apoyos Publicitarios",
    "ctaMayor": "5204000000"
  },
  {
    "codigo": "5204060001",
    "nombre": "Ingresos por Apoyos Publicitarios",
    "ctaMayor": "5204060000"
  },
  {
    "codigo": "5204070000",
    "nombre": "Ingresos por Indemnizacion de Seguros",
    "ctaMayor": "5204000000"
  },
  {
    "codigo": "5204070001",
    "nombre": "Ingresos por Indemnizacion de Seguros",
    "ctaMayor": "5204070000"
  },
  {
    "codigo": "5204990000",
    "nombre": "Otros Ingresos",
    "ctaMayor": "5204000000"
  },
  {
    "codigo": "5204990001",
    "nombre": "Otros Ingresos",
    "ctaMayor": "5204990000"
  },
  {
    "codigo": "5300000000",
    "nombre": "GASTO CONTABLE",
    "ctaMayor": "5000000000"
  },
  {
    "codigo": "5301000000",
    "nombre": "GASTO POR DEPRECIACION",
    "ctaMayor": "5300000000"
  },
  {
    "codigo": "5301010000",
    "nombre": "Gasto Depreciacion Mejoras A Inmuebles",
    "ctaMayor": "5301000000"
  },
  {
    "codigo": "5301010001",
    "nombre": "Gasto Depreciacion Mejoras A Inmuebles",
    "ctaMayor": "5301010000"
  },
  {
    "codigo": "5301020000",
    "nombre": "Gasto Depreciacion Mobiliario y Equipo Oficina",
    "ctaMayor": "5301000000"
  },
  {
    "codigo": "5301020001",
    "nombre": "Gasto Depreciacion Mobiliario y Equipo Oficina",
    "ctaMayor": "5301020000"
  },
  {
    "codigo": "5301030000",
    "nombre": "Gasto Depreciacion Maquinaria Y Equipo Operacion",
    "ctaMayor": "5301000000"
  },
  {
    "codigo": "5301030001",
    "nombre": "Gasto Depreciacion Maquinaria Y Equipo Operacion",
    "ctaMayor": "5301030000"
  },
  {
    "codigo": "5301040000",
    "nombre": "Gasto Depreciacion Equipo De Transporte",
    "ctaMayor": "5301000000"
  },
  {
    "codigo": "5301040001",
    "nombre": "Gasto Depreciacion Equipo De Transporte",
    "ctaMayor": "5301040000"
  },
  {
    "codigo": "5301050000",
    "nombre": "Gasto Depreciacion Equipo De Computo",
    "ctaMayor": "5301000000"
  },
  {
    "codigo": "5301050001",
    "nombre": "Gasto Depreciacion Equipo De Computo",
    "ctaMayor": "5301050000"
  },
  {
    "codigo": "5301060000",
    "nombre": "Gasto Depreciacion Equipo De Comunicacion",
    "ctaMayor": "5301000000"
  },
  {
    "codigo": "5301060001",
    "nombre": "Gasto Depreciacion Equipo De Comunicacion",
    "ctaMayor": "5301060000"
  },
  {
    "codigo": "5301070000",
    "nombre": "Gasto Depreciacion Otros Activos Fijos",
    "ctaMayor": "5301000000"
  },
  {
    "codigo": "5301070001",
    "nombre": "Gasto Depreciacion Otros Activos Fijos",
    "ctaMayor": "5301070000"
  },
  {
    "codigo": "5301080000",
    "nombre": "Gasto Depreciacion Edificios",
    "ctaMayor": "5301000000"
  },
  {
    "codigo": "5301080001",
    "nombre": "Gasto Depreciacion Edificios",
    "ctaMayor": "5301080000"
  },
  {
    "codigo": "5302000000",
    "nombre": "GASTO POR AMORTIZACION",
    "ctaMayor": "5300000000"
  },
  {
    "codigo": "5302010000",
    "nombre": "Gasto Por Amortizacion",
    "ctaMayor": "5302000000"
  },
  {
    "codigo": "5302010001",
    "nombre": "Gasto Por Amortizacion",
    "ctaMayor": "5302010000"
  },
  {
    "codigo": "6000000000",
    "nombre": "IMPUESTOS DEL EJERCICIO",
    "ctaMayor": null
  },
  {
    "codigo": "6000010000",
    "nombre": "I.S.R. Del Ejercicio",
    "ctaMayor": "6000000000"
  },
  {
    "codigo": "6000010001",
    "nombre": "I.S.R. Del Ejercicio",
    "ctaMayor": "6000010000"
  },
  {
    "codigo": "6000020000",
    "nombre": "P.T.U. Del Ejercicio",
    "ctaMayor": "6000000000"
  },
  {
    "codigo": "6000020001",
    "nombre": "P.T.U. Del Ejercicio",
    "ctaMayor": "6000020000"
  },
  {
    "codigo": "7000000000",
    "nombre": "CUENTAS DE ORDEN",
    "ctaMayor": null
  },
  {
    "codigo": "7000010000",
    "nombre": "CUFIN De Ejercicios Anteriores",
    "ctaMayor": "7000000000"
  },
  {
    "codigo": "7000010001",
    "nombre": "CUFIN De Ejercicios Anteriores",
    "ctaMayor": "7000010000"
  },
  {
    "codigo": "7000010002",
    "nombre": "Contra Cuenta CUFIN Ejercicios Anteriores",
    "ctaMayor": "7000010000"
  },
  {
    "codigo": "7000020000",
    "nombre": "CUCA De Ejercicios Anteriores",
    "ctaMayor": "7000000000"
  },
  {
    "codigo": "7000020001",
    "nombre": "CUCA De Ejercicios Anteriores",
    "ctaMayor": "7000020000"
  },
  {
    "codigo": "7000020002",
    "nombre": "Contra Cuenta CUCA De Ejercicios Anteriores",
    "ctaMayor": "7000020000"
  },
  {
    "codigo": "7000030000",
    "nombre": "Ajuste Anual Por Inflacion Acumulable",
    "ctaMayor": "7000000000"
  },
  {
    "codigo": "7000030001",
    "nombre": "Ajuste Anual por Inflacion Acumulable",
    "ctaMayor": "7000030000"
  },
  {
    "codigo": "7000030002",
    "nombre": "Acumulacion del Ajuste Anual Inflacionario",
    "ctaMayor": "7000030000"
  },
  {
    "codigo": "7000040000",
    "nombre": "Deducción De Inversión",
    "ctaMayor": "7000000000"
  },
  {
    "codigo": "7000040001",
    "nombre": "Deducción de Inversión",
    "ctaMayor": "7000040000"
  },
  {
    "codigo": "7000040002",
    "nombre": "Contra Cuenta Deducción de Inversiones",
    "ctaMayor": "7000040000"
  },
  {
    "codigo": "7000050000",
    "nombre": "CUFIN Del Ejercicio",
    "ctaMayor": "7000000000"
  },
  {
    "codigo": "7000050001",
    "nombre": "CUFIN",
    "ctaMayor": "7000050000"
  },
  {
    "codigo": "7000050002",
    "nombre": "Contra Cuenta CUFIN",
    "ctaMayor": "7000050000"
  },
  {
    "codigo": "7000060000",
    "nombre": "CUCA Del Ejercicio",
    "ctaMayor": "7000000000"
  },
  {
    "codigo": "7000060001",
    "nombre": "CUCA",
    "ctaMayor": "7000060000"
  },
  {
    "codigo": "7000060002",
    "nombre": "Contra Cuenta CUCA",
    "ctaMayor": "7000060000"
  },
  {
    "codigo": "7000070000",
    "nombre": "UFIN Del Ejercicio",
    "ctaMayor": "7000000000"
  },
  {
    "codigo": "7000070001",
    "nombre": "UFIN",
    "ctaMayor": "7000070000"
  },
  {
    "codigo": "7000070002",
    "nombre": "Contra Cuenta UFIN",
    "ctaMayor": "7000070000"
  },
  {
    "codigo": "7000080000",
    "nombre": "Utilidad o Perdida Fiscal en Venta de Activo Fijo",
    "ctaMayor": "7000000000"
  },
  {
    "codigo": "7000080001",
    "nombre": "Utilidad o Perdida Fiscal en Venta de Activo Fijo",
    "ctaMayor": "7000080000"
  },
  {
    "codigo": "7000080002",
    "nombre": "Contra Cuenta Utilidad o Perdida Fiscal Venta AF",
    "ctaMayor": "7000080000"
  }
];

// ── Centros de costo (13 registros) ──────────────────────────────────────────
const CENTROS_COSTO = [
  {
    "clave": "111",
    "sucursal": "HIDALGO",
    "serieFacturacion": "B0"
  },
  {
    "clave": "112",
    "sucursal": "REFORMA",
    "serieFacturacion": "D0"
  },
  {
    "clave": "113",
    "sucursal": "SIMBOLOS",
    "serieFacturacion": "G0"
  },
  {
    "clave": "114",
    "sucursal": "ATZOMPA",
    "serieFacturacion": "E0"
  },
  {
    "clave": "115",
    "sucursal": "FERROCARRIL",
    "serieFacturacion": "F0"
  },
  {
    "clave": "116",
    "sucursal": "TEHUANTEPEC",
    "serieFacturacion": "H0"
  },
  {
    "clave": "117",
    "sucursal": "SANTA ROSA",
    "serieFacturacion": "M0"
  },
  {
    "clave": "119",
    "sucursal": "VIGUERA",
    "serieFacturacion": "N0"
  },
  {
    "clave": "120",
    "sucursal": "PUERTO ESCONDIDO",
    "serieFacturacion": "O0"
  },
  {
    "clave": "211",
    "sucursal": "CONSTRUCASA",
    "serieFacturacion": "C0"
  },
  {
    "clave": "212",
    "sucursal": "PROMOTORIA",
    "serieFacturacion": "I0"
  },
  {
    "clave": "214",
    "sucursal": "LICITACION HIDALGO",
    "serieFacturacion": "G1"
  },
  {
    "clave": "300",
    "sucursal": "CEDIS",
    "serieFacturacion": "A0"
  }
];

// ── Helpers ───────────────────────────────────────────────────────────────────

async function seedCuentas() {
  // Primer paso: upsert de todas las cuentas sin parentId
  for (const c of CUENTAS) {
    await AccountPlan.upsert(
      { codigo: c.codigo, nombre: c.nombre, ctaMayor: c.ctaMayor },
      { conflictFields: ['codigo'] }
    );
  }

  // Segundo paso: resolver parentId a partir de ctaMayor
  const todas = await AccountPlan.findAll({ attributes: ['id', 'codigo'], raw: true });
  const mapaId = Object.fromEntries(todas.map(r => [r.codigo, r.id]));

  for (const c of CUENTAS) {
    if (c.ctaMayor && mapaId[c.ctaMayor]) {
      await AccountPlan.update(
        { parentId: mapaId[c.ctaMayor] },
        { where: { codigo: c.codigo } }
      );
    }
  }

  console.log();
}

async function seedCentrosCosto() {
  for (const c of CENTROS_COSTO) {
    await CentroCosto.upsert(c, { conflictFields: ['clave'] });
  }
  console.log();
}

async function seedAccountPlan() {
  await seedCuentas();
  await seedCentrosCosto();
}

// ── Ejecución directa ─────────────────────────────────────────────────────────
if (require.main === module) {
  const { connectPostgres, disconnectPostgres } = require('../../config/database.postgres');
  connectPostgres()
    .then(async () => {
      await seedAccountPlan();
      await disconnectPostgres();
      process.exit(0);
    })
    .catch(err => {
      console.error('[seed-account-plan] Error:', err.message);
      process.exit(1);
    });
}

module.exports = seedAccountPlan;
