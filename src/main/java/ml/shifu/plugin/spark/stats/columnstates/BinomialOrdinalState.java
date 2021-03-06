/**
 * Copyright [2012-2014] eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.plugin.spark.stats.columnstates;

import java.util.ArrayList;

import ml.shifu.core.container.CategoricalValueObject;
import ml.shifu.core.util.Params;
import ml.shifu.plugin.spark.stats.interfaces.ColumnState;
import ml.shifu.plugin.spark.stats.interfaces.UnitState;
import ml.shifu.plugin.spark.stats.unitstates.FrequencyUnitState;

/**
 * Similar to SimpleUnivariateOrdinalState, with the exception that it recieves data in the form of CategoricalValueObjects
 * which it unpacks to consider only the value fields.
 * 
 */
public class BinomialOrdinalState extends ColumnState {

    public BinomialOrdinalState(String name, Params parameters) {
        states= new ArrayList<UnitState>();
        states.add(new FrequencyUnitState());
        params= parameters;
        fieldName= name;
    }

    @Override
    public ColumnState getNewBlank() {
        return new BinomialOrdinalState(fieldName, params);
    }

    @Override
    public void checkClass(ColumnState colState) throws Exception {
        if(!(colState instanceof BinomialOrdinalState)) 
            throw new Exception("Expected BinomialOrdinalState, got " + colState.getClass().toString());
    }
    
}
