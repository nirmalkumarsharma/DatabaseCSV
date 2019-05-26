package in.nks.configuration;

import java.net.MalformedURLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.UrlResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import in.nks.model.Employee;
import in.nks.processor.EmployeeProcessor;

@Configuration
@EnableBatchProcessing
public class BatchJobConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
	@Bean
	public DataSource getDataSource()
	{
		final DriverManagerDataSource dataSource=new DriverManagerDataSource();
		dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
		dataSource.setUrl("jdbc:mysql://localhost:3306/national_electric");
		dataSource.setUsername("root");
		dataSource.setPassword("abcd1234");
		return dataSource;
	}
	
	@Bean
	public FlatFileItemReader<Employee> reader() throws MalformedURLException{
		FlatFileItemReader<Employee> reader=new FlatFileItemReader<>();
		reader.setResource(new UrlResource("file:/media/nirmal/3E9C39F64CC16D4A/National_Electric.csv"));
		reader.setLineMapper(new DefaultLineMapper<Employee>() {{
			setLineTokenizer(new DelimitedLineTokenizer() {{
				setNames(new String[] {"firstName", "lastName", "department"});
				setDelimiter(",");
			}});
			setFieldSetMapper(new BeanWrapperFieldSetMapper<Employee>() {{
				setTargetType(Employee.class);
			}});
		}});
		return reader;
	}
	
	@Bean
	public EmployeeProcessor processor() {
		return new EmployeeProcessor();
	}
	
	@Bean
	public JdbcBatchItemWriter<Employee> writer() {
		JdbcBatchItemWriter<Employee> writer=new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Employee>());
		writer.setSql("INSERT INTO employee (first_name, last_name, department) VALUES (:firstName, :lastName, :department)");
		writer.setDataSource(dataSource);
		return writer;
	}
	
	@Bean
	public Step step() throws MalformedURLException {
		return stepBuilderFactory.get("step").<Employee, Employee> chunk(2).reader(reader()).processor(processor()).writer(writer()).build();
	}
	
	@Bean
	public Job importEmployeeJob() throws MalformedURLException {
		return jobBuilderFactory.get("importEmployeeJob").incrementer(new RunIdIncrementer()).flow(step()).end().build();
	}
}
