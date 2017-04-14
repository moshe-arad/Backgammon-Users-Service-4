package org.moshe.arad.entities;

import java.util.Date;

public class BackgammonUser {

	private Long userId;
	private String userName;
	private String password;
	private Boolean enabled;
	private String firstName;
	private String lastName;
	private String email;
	private Date lastModifiedDate;
	private String lastModifiedBy;
	private Date createdDate;
	private String createdBy;

	public BackgammonUser() {
	}
	
	public BackgammonUser(String userName, String password, Boolean enabled, String firstName, String lastName,
			String email, Date lastModifiedDate, String lastModifiedBy, Date createdDate, String createdBy) {
		super();
		this.userName = userName;
		this.password = password;
		this.enabled = enabled;
		this.firstName = firstName;
		this.lastName = lastName;
		this.email = email;
		this.lastModifiedDate = lastModifiedDate;
		this.lastModifiedBy = lastModifiedBy;
		this.createdDate = createdDate;
		this.createdBy = createdBy;
	}

	@Override
	public String toString() {
		return "BackgammonUser [userName=" + userName + ", password=" + password + ", enabled=" + enabled
				+ ", firstName=" + firstName + ", lastName=" + lastName + ", email=" + email + ", lastModifiedDate="
				+ lastModifiedDate + ", lastModifiedBy=" + lastModifiedBy + ", createDate=" + createdDate
				+ ", createdBy=" + createdBy + "]";
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Boolean getEnabled() {
		return enabled;
	}

	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}
	
	public Date getLastModifiedDate() {
		return lastModifiedDate;
	}

	public void setLastModifiedDate(Date lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	public String getLastModifiedBy() {
		return lastModifiedBy;
	}

	public void setLastModifiedBy(String lastModifiedBy) {
		this.lastModifiedBy = lastModifiedBy;
	}

	public Date getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Date createDate) {
		this.createdDate = createDate;
	}

	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
}
