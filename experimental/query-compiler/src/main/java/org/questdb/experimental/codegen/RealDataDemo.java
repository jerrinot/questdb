/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package org.questdb.experimental.codegen;

import io.questdb.cairo.*;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import java.nio.file.*;
import java.util.List;
import static org.questdb.experimental.codegen.Pipeline.*;

/** Real-data validation on the numeric columns of UCI Adult, with original row order and correlations. */
public final class RealDataDemo {
    public static void main(String[] args)throws Exception {
        if(args.length==0)throw new IllegalArgumentException("Usage: RealDataDemo /path/to/adult.data [repetitions]");
        int repetitions=args.length>1?Integer.parseInt(args[1]):9;
        if(repetitions<3)throw new IllegalArgumentException("At least three repetitions required");
        var database=Files.createTempDirectory("questdb-codegen-real-");
        try {
            var config=new DefaultCairoConfiguration(database.toString());
            try(var engine=new CairoEngine(config);var context=new SqlExecutionContextImpl(engine,1).with(
                    config.getFactoryProvider().getSecurityContextFactory().getRootContext(),null,null,-1,null)) {
                engine.execute("create table input(age int,education short,gain long,hours int)",context);
                int rows=0;
                try(var writer=engine.getWriter(engine.getTableTokenIfExists("input"),"prototype")) {
                    for(String line:Files.readAllLines(Path.of(args[0]))) {
                        if(line.isBlank()||line.startsWith("|"))continue;
                        var fields=line.split(",");if(fields.length!=15)throw new IllegalArgumentException("Unexpected Adult row");
                        var row=writer.newRow();
                        row.putInt(0,Integer.parseInt(fields[0].strip()));row.putShort(1,Short.parseShort(fields[4].strip()));
                        row.putLong(2,Long.parseLong(fields[10].strip()));row.putInt(3,Integer.parseInt(fields[12].strip()));
                        row.append();rows++;
                    }
                    writer.commit();
                }
                System.out.println("UCI Adult: "+rows+" rows; four numeric columns; no duplication or reordering.");
                Column age=new Column(0,"age",ColumnType.INT),edu=new Column(1,"education",ColumnType.SHORT),
                        gain=new Column(2,"gain",ColumnType.LONG),hours=new Column(3,"hours",ColumnType.INT);
                var p=new Pipeline(List.of(age,edu,gain,hours),new SqlCompare(Ir.Op.GE,hours,new Constant(40)),List.of(age,edu),
                        List.of(new Count(),new Sum(gain),new NestedCountDistinct(hours)));
                WorkloadMatrix.clearDirectory(Path.of("experimental/query-compiler/target/matrix-code"));
                WorkloadMatrix.measure(engine,context,new WorkloadMatrix.Spec("adult-multiple-keys",null,
                        "select age,education,count(*),sum(gain),count_distinct(hours) from input where hours>=40 group by age,education",p,null,null),repetitions,200);
                p=new Pipeline(List.of(age,edu,gain,hours),new All(),List.of(edu),List.of(new Count(),new Sum(gain),new NestedCountDistinct(hours)));
                WorkloadMatrix.clearDirectory(Path.of("experimental/query-compiler/target/matrix-code"));
                WorkloadMatrix.measure(engine,context,new WorkloadMatrix.Spec("adult-low-cardinality",null,
                        "select education,count(*),sum(gain),count_distinct(hours) from input group by education",p,null,null),repetitions,200);
            }
        }finally{LogFactory.haltInstance();WorkloadMatrix.clearDirectory(database);}
    }
}
