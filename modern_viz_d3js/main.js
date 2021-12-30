const alto = 700
const ancho = 800
const barPadding = 0.05
const margen = {
    izquierdo: 40,
    derecho: 10,
    superior: 10,
    inferior: 40,
}


const svg = d3.select("#chart").append("svg").attr("id", "svg").attr("width", ancho).attr("height", alto)
const grupoElementos = svg.append("g").attr("id", "grupoElementos")
    .attr("transform", `translate(${margen.izquierdo}, ${margen.superior})`)

var x1 = d3.scaleBand().rangeRound([0, ancho - margen.izquierdo - margen.derecho]).padding(barPadding)
var y = d3.scaleLinear().rangeRound([alto - margen.superior - margen.inferior, 0])



const grupoEjes = svg.append("g").attr("id", "grupoEjes")
const grupoX = grupoEjes.append("g").attr("id", "grupoX")
    .attr("transform", `translate(${margen.izquierdo}, ${alto - margen.inferior})`)
const grupoY = grupoEjes.append("g").attr("id", "grupoY")
    .attr("transform", `translate(${margen.izquierdo}, ${margen.superior})`)

const ejeX = d3.axisBottom().scale(x1)
const ejeY = d3.axisLeft().scale(y)



d3.csv("data.csv").then(datos0 => {
    datos0.map(d => {
        d.age = +d.age
      })

    console.log(datos0)

    var datos1 = [];
      function años(year) {
          for (var i=1998; i < 2020; ++i){
              const diCaprioBirthYear = 1974;
              var datas1 = {}; 
              datas1.nameDi = "DiCaprio";
              datas1.ageDi = i - diCaprioBirthYear;
              datas1.year= i;
                  datos1.push(datas1)
          }
         return datos1;
      }
      
      años(datos0.year)



    const datos2 = datos0.map(item => {
        const obj = datos1.find(o => o.year === parseInt(item.year));
        return { ...item, ...obj };
      });
    
    console.log(datos2);

    

    y.domain([d3.min(datos2.map(dato => dato.age)) -18, d3.max(datos2.map(dato => dato.age)) + 20])
   
    x1.domain(datos2.map( d => d.year))

    grupoX.call(ejeX)
    grupoY.call(ejeY)

  
    var elementos = grupoElementos.selectAll(".bar").data( datos2).enter()

    elementos.append("rect")
        .attr("height", d => alto - margen.superior - margen.inferior- y(d.age)) 
        .attr("y", d => y(d.age)) 
        .attr("x", d => x1(d.year)) 
        .attr("width", x1.bandwidth())  
        .attr("fill", "grey")
        .attr("fill-opacity", 0.5)
        
    elementos.append("text")
        .attr("font-family", "sans-serif")
        .attr("fill", "black")
        .attr("x", d=>x1(d.year))
        .attr("y", d=>y(d.age))
        .text(d=>d.name)
        .attr("text-anchor","middle")
        .attr("transform", "rotate(1800)")
        .style("alignment-baseline", "middle")

  
  var lineData = datos2.filter(f => f.nameDi === "DiCaprio");
  

  var   yLineScale = d3.scaleLinear()
    .rangeRound([alto- margen.superior - margen.inferior, 0])
    .domain([0, d3.max(lineData, (function (d) {
      return d.ageDi;
    }))]);
    
  grupoElementos.append("g")
      .call(d3.axisRight(yLineScale));

  var   yLineScale = d3.scaleLinear()
      .rangeRound([alto, 0])
      .domain([0, d3.max(lineData, (function (d) {
        return d.ageDi;
      }))]);
  
  let lineChart = grupoElementos.append('g')
      .attr('class', 'lineChart') ;

  lineChart.selectAll('circle').data(lineData).enter().append("circle")
      .attr("class", "dot") 
      .attr("cx", function(d, i) { return x1(d.year)+ x1.bandwidth() /2})
      .attr("cy", function(d) { return yLineScale(d.ageDi); })
      .attr("r", 5);
    var line = d3.line()
      .x(function(d,i) { return x1(d.year) + x1.bandwidth() / 2})
      .y(function(d) { return yLineScale(d.ageDi)})
      .curve(d3.curveMonotoneX);

  lineChart.append("path")
    .attr("class", "line") 
    .attr("d", line(lineData)); 
            
   lineChart.selectAll('text').data(lineData).enter().append("text")
       .attr("x", function(d) { return x1(d.year) + x1.bandwidth() / 2; })
       .attr("y", function(d) { return yLineScale(d.ageDi); })
       .attr("text-anchor", "middle")
       .text(function(d) {
           return parseInt(d.ageDi);
       });


})




