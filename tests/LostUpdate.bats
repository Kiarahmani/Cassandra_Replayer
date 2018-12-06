#!/usr/bin/env zunit
# 
@setup {
  # Load a script to set up the test environment
  load tests_scprits.sh
}

@test '(1) LOST UPDATE anomaly occured' {
  run lu
  assert "$output" equals "(1,100)"
}
@test '(2) LOST UPDATE anomaly occured' {
  run lu
  assert "$output" equals "(1,100)"
}
@test '(3) LOST UPDATE anomaly occured' {
  run lu
  assert "$output" equals "(1,100)"
}
@test '(4) LOST UPDATE anomaly occured' {
  run lu
  assert "$output" equals "(1,100)"
}
@test '(5) LOST UPDATE anomaly occured' {
  run lu
  assert "$output" equals "(1,100)"
}
