/*
 * This is the class which contains all the variables which are broadcast to the worker nodes. 
 * An object of this class is created and wrapped into a broadcast variable using JavaSparkContext.broadcast().
 * The resulting broadcast variable is passed to all Normalizers running on the worker nodes. 
 * The Normalizers unpack the BroadcastVariables object and use the contained variables.
 */
package ml.shifu.norm;

import java.util.List;

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
