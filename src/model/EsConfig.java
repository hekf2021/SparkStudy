package model;

public class EsConfig {
	private String elasticsearchurl="10.111.134.54:9500";
	private String clustername="elasticsearch";

	public String getElasticsearchurl() {
		return elasticsearchurl;
	}

	public void setElasticsearchurl(String elasticsearchurl) {
		this.elasticsearchurl = elasticsearchurl;
	}

	public String getClustername() {
		return clustername;
	}

	public void setClustername(String clustername) {
		this.clustername = clustername;
	}

}
