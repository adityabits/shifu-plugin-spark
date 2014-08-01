package ml.shifu.norm;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.PMML;

import ml.shifu.core.di.builtin.transform.DefaultTransformationExecutor;

public class BroadcastVariables {

	private Broadcast<DefaultTransformationExecutor> exec;
	private Broadcast<PMML> pmml;
	private Broadcast<List<DataField>> dataFields;
	private Broadcast<List<DerivedField>> activeFields;
	private Broadcast<List<DerivedField>> targetFields;

	public BroadcastVariables(JavaSparkContext jsc, DefaultTransformationExecutor executor, PMML pmml, List<DataField> dataFields, List<DerivedField> activeFields, List<DerivedField> targetFields) {
		// TODO Auto-generated constructor stub
		this.exec= jsc.broadcast(executor);
		this.pmml= jsc.broadcast(pmml);
		this.dataFields= jsc.broadcast(dataFields);
		this.activeFields= jsc.broadcast(activeFields);
		this.targetFields= jsc.broadcast(targetFields);
	}

	public PMML getPmml() {
		return pmml.value();
	}

	public List<DataField> getDataFields() {
		return dataFields.value();
	}

	public List<DerivedField> getTargetFields() {
		return targetFields.value();
	}

	public List<DerivedField> getActiveFields() {
		return activeFields.value();
	}

	public DefaultTransformationExecutor getExec() {
		return exec.value();
	}

}
