package in.nks.processor;

import org.springframework.batch.item.ItemProcessor;

import in.nks.model.Employee;

public class EmployeeProcessor implements ItemProcessor<Employee, Employee>{

	@Override
	public Employee process(Employee employee) throws Exception {
		return employee;
	}

}
