#
# Cookbook:: web-server-apache
# Recipe:: default
#
# Copyright:: 2018, The Authors, All Rights Reserved.


# install package with apt/dpkg
package 'apache2'

service 'apache2' do
  supports status: true
  action [:enable, :start]
end

file '/var/www/html/index.html' do
  content 
  '<html>
	  <body>
	  	<h1>Life is not about how hard of a hit you can give... it is about how many you can take, and still keep moving forward. </h1>
	  </body>
</html>'
end
