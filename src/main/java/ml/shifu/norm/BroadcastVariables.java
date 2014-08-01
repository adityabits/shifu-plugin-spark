package ml.shifu.norm;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DerivedField;
import org.dmg.pmml.PMML;

import ml.shifu.core.di.builtin.transform.DefaultTransformationExecutor;

public class BroadcastVariables {

	private DefaultTransformationExecutor exec;
	private PMML pmml;
	private List<DataField> dataFields;
	private List<DerivedField> activeFields;
	private List<DerivedField> targetFields;

	public BroadcastVariables(DefaultTransformationExecutor executor, PMML pmml, List<DataField> dataFields, List<DerivedField> activeFields, List<DerivedField> targetFields) {
		// TODO Auto-generated constructor stub
		this.exec= executor;
		this.pmml= pmml;
		this.dataFields= dataFields;
		this.activeFields= activeFields;
		this.targetFields= targetFields;
	}

	public PMML getPmml() {
		return pmml;
	}

	public List<DataField> getDataFields() {
		return dataFields;
	}

	public List<DerivedField> getTargetFields() {
		return targetFields;
	}

	public List<DerivedField> getActiveFields() {
		return activeFields;
	}

	public DefaultTransformationExecutor getExec() {
		return exec;
	}

}
