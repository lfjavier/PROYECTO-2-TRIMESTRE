/*comision a pagar de cada pedido promocionado*/

db.pedidos.aggregate([
        {
            $match:{
                promocionado:true
            }
        },
        {
            $lookup:{
                from:"embajadores",
                localField:"Cod_Emb",
                foreignField:"Cod_Emb",
                as:"PROMOCIONES",
             }
         },
        {
            $project:{
                 _id:0,
                 Id_Venta:1,
                 Fecha_venta:{$year:"$Fecha_venta"},
                 Importe_Pedido:1,
                 Embajador:"$PROMOCIONES.Nom_Emb",
                 porcentaje:{$arrayElemAt:["$PROMOCIONES.Porcentaje",0]},
                 comision:{$multiply:["$Importe_Pedido",{$arrayElemAt:["$PROMOCIONES.Porcentaje",0]}]},
               }
        },   
     ])

/* comision total por años.........................*/
db.pedidos.aggregate([
    {
        $match:{
            promocionado:true
        }
    },
    {
        $group:{
           _id:{año: { $year: "$Fecha_venta" },Cod_Emb:"$Cod_Emb" }
        }
    },
    {
        $lookup:{
            from:"embajadores",
            localField:"Cod_Emb",
            foreignField:"Cod_Emb",
            as:"PROMOCIONES",
         }
     },
    
 ])



/*comision total de un embajador en concreto*/

db.embajadores.aggregate([
    {
        $lookup:{
            from:"pedidos",
            localField:"Cod_Emb",
            foreignField:"Cod_Emb",
            as:"Ppromocionado",
         }
    },
    {
        $match:{
            Nom_Emb:"JUANFARO_PRO"
        }
    },
    {
        $project:{
            _id:0,
            Nom_Emb:1,
            ventas:"$Ppromocionado.Id_Venta",
            Fecha_venta:"$Ppromocionado.Fecha_venta",
            importe_venta1:{$arrayElemAt:["$Ppromocionado.Importe_Pedido",0]},
            importe_venta2:{$arrayElemAt:["$Ppromocionado.Importe_Pedido",1]},
            comision1:{$multiply:["$Porcentaje",{$arrayElemAt:["$Ppromocionado.Importe_Pedido",0]}]},
            comision2:{$multiply:["$Porcentaje",{$arrayElemAt:["$Ppromocionado.Importe_Pedido",1]}]},
        }
    },
    { 
        $project:
        { 
            Nom_Emb:1,
            ventas:1,
            Fecha_venta:1,
            comision_total:{$sum:["$comision1","$comision2"]}
        }
        
    }
])


/* comision de cada embajador .*/

db.embajadores.aggregate([
    {
        $lookup:{
            from:"pedidos",
            localField:"Cod_Emb",
            foreignField:"Cod_Emb",
            as:"promocionado",
         }
    },
    {
        $group:{
            _id:{embajador:"$Nom_Emb" ,porcentaje:"$Porcentaje", pedido:"$promocionado.Id_Venta", importepedido:"$promocionado.Importe_Pedido"},
        }
    },
    {
        $unwind:"$_id.importepedido"
    },
    {
        $project:{
            _id:0,
            embajador:"$_id.embajador",
            pedido:"$_id.pedido",
            comision:{$multiply:["$_id.porcentaje","$_id.importepedido"]},

        }
    },

])


/*peso total de cada pedido */


db.pedidos.aggregate([
    {
        $lookup:{
            from:"productos",
            localField:"Productos.Cod_Producto",
            foreignField:"Cod_Producto",
            as:"PESO",
         }
    },
    {
        $project:{
            _id:0,
            Id_Venta:1,
            Num_Productos:1,
            productos:"$Productos.Cod_Producto",
            PESO:"$PESO.peso",
            peso_total:{$sum:"$PESO.peso"}
        }
    },  
])

/* Precio  del envio de cada pedido (suponiendo que el kg cuesta 12,04 euros) */
db.pedidos.aggregate([
    {
        $lookup:{
            from:"productos",
            localField:"Productos.Cod_Producto",
            foreignField:"Cod_Producto",
            as:"PESO",
         }
    },
    {
        $project:{
            _id:0,
            Id_Venta:1,
            Num_Productos:1,
            productos:"$Productos.Cod_Producto",
            PESO:"$PESO.peso",
            peso_total:{$sum:"$PESO.peso"}
        }
    },
    {
        $project:{
            Id_Venta:1,
            peso_total:1,
            coste_envio:{$multiply:["$peso_total",0.01204]}
        }   
    }  
])











   
